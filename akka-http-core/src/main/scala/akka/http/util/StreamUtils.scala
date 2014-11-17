/*
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.http.util

import java.util.concurrent.atomic.AtomicBoolean
import java.io.InputStream
import org.reactivestreams.{ Subscriber, Publisher }
import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import akka.stream.impl.fusing.DeterministicOp
import akka.stream.impl.fusing.Directive
import akka.stream.impl.fusing.Context
import akka.actor.Props
import akka.util.ByteString
import akka.stream.{ impl, FlowMaterializer }
import akka.stream.scaladsl._
import akka.http.model.RequestEntity
import akka.stream.impl.fusing.TransitivePullOp
import akka.stream.impl.fusing.RichDeterministicOp
import akka.stream.impl.fusing.TerminationDirective
import akka.stream.scaladsl.Pipe
import akka.stream.impl.fusing.IteratorInterpreter
import scala.collection.mutable.ListBuffer
import akka.stream.impl.Ast.AstNode
import akka.stream.impl.Ast.OpFactory
import scala.annotation.tailrec
import scala.util.Try

/**
 * INTERNAL API
 */
private[http] object StreamUtils {

  /**
   * Creates a transformer that will call `f` for each incoming ByteString and output its result. After the complete
   * input has been read it will call `finish` once to determine the final ByteString to post to the output.
   */
  def byteStringTransformer(f: ByteString ⇒ ByteString, finish: () ⇒ ByteString): Flow[ByteString, ByteString] = {
    val transformer = new DeterministicOp[ByteString, ByteString] {
      override def onPush(element: ByteString, ctxt: Context[ByteString]): Directive =
        ctxt.push(f(element))

      override def onPull(ctxt: Context[ByteString]): Directive =
        if (isFinishing) ctxt.pushAndFinish(finish())
        else ctxt.pull()

      override def onUpstreamFinish(ctxt: Context[ByteString]): TerminationDirective = ctxt.absorbTermination()
    }
    Flow[ByteString].transform("transformBytes", () ⇒ transformer)
  }

  def failedPublisher[T](ex: Throwable): Publisher[T] =
    impl.ErrorPublisher(ex).asInstanceOf[Publisher[T]]

  def mapErrorTransformer(f: Throwable ⇒ Throwable): Flow[ByteString, ByteString] = {
    val transformer = new TransitivePullOp[ByteString, ByteString] {
      override def onPush(element: ByteString, ctxt: Context[ByteString]): Directive =
        ctxt.push(element)

      override def onFailure(cause: Throwable, ctxt: Context[ByteString]): TerminationDirective =
        ctxt.fail(f(cause))
    }

    Flow[ByteString].transform("transformError", () ⇒ transformer)
  }

  def sliceBytesTransformer(start: Long, length: Long): Flow[ByteString, ByteString] = {
    val transformer = new RichDeterministicOp[ByteString, ByteString] {

      def skipping = new State {
        var toSkip = start
        override def onPush(element: ByteString, ctxt: Context[ByteString]): Directive =
          if (element.length < toSkip) {
            // keep skipping
            toSkip -= element.length
            ctxt.pull()
          } else {
            become(taking(length))
            // toSkip <= element.length <= Int.MaxValue
            current.onPush(element.drop(toSkip.toInt), ctxt)
          }
      }
      def taking(initiallyRemaining: Long) = new State {
        var remaining: Long = initiallyRemaining
        override def onPush(element: ByteString, ctxt: Context[ByteString]): Directive = {
          val data = element.take(math.min(remaining, Int.MaxValue).toInt)
          println(s"# onPush remaining: $remaining element: $element data: $data") // FIXME
          remaining -= data.size
          if (remaining <= 0) ctxt.pushAndFinish(data)
          else ctxt.push(data)
        }
      }

      override def initial: State = if (start > 0) skipping else taking(length)
    }
    Flow[ByteString].transform("sliceBytes", () ⇒ transformer)
  }

  /**
   * Applies a sequence of transformers on one source and returns a sequence of sources with the result. The input source
   * will only be traversed once.
   */
  def transformMultiple(input: Source[ByteString], transformers: immutable.Seq[Flow[ByteString, ByteString]])(implicit materializer: FlowMaterializer): immutable.Seq[Source[ByteString]] =
    transformers match {
      case Nil      ⇒ Nil
      case Seq(one) ⇒ Vector(input.via(one))
      case multiple ⇒
        val results = Vector.fill(multiple.size)(Sink.publisher[ByteString])
        val mat =
          FlowGraph { implicit b ⇒
            import FlowGraphImplicits._

            val broadcast = Broadcast[ByteString]("transformMultipleInputBroadcast")
            input ~> broadcast
            (multiple, results).zipped.foreach { (trans, sink) ⇒
              broadcast ~> trans ~> sink
            }
          }.run()
        results.map(s ⇒ Source(mat.get(s)))
    }

  def mapEntityError(f: Throwable ⇒ Throwable): RequestEntity ⇒ RequestEntity =
    _.transformDataBytes(mapErrorTransformer(f))

  /**
   * Simple blocking Source backed by an InputStream.
   *
   * FIXME: should be provided by akka-stream, see #15588
   */
  def fromInputStreamSource(inputStream: InputStream, defaultChunkSize: Int = 65536): Source[ByteString] = {
    import akka.stream.impl._

    def props(materializer: ActorBasedFlowMaterializer): Props = {
      val iterator = new Iterator[ByteString] {
        var finished = false
        def hasNext: Boolean = !finished
        def next(): ByteString =
          if (!finished) {
            val buffer = new Array[Byte](defaultChunkSize)
            val read = inputStream.read(buffer)
            if (read < 0) {
              finished = true
              inputStream.close()
              ByteString.empty
            } else ByteString.fromArray(buffer, 0, read)
          } else ByteString.empty
      }

      Props(new IteratorPublisherImpl(iterator, materializer.settings)).withDispatcher(materializer.settings.fileIODispatcher)
    }

    new AtomicBoolean(false) with SimpleActorFlowSource[ByteString] {
      override def attach(flowSubscriber: Subscriber[ByteString], materializer: ActorBasedFlowMaterializer, flowName: String): Unit =
        create(materializer, flowName)._1.subscribe(flowSubscriber)

      override def isActive: Boolean = true
      override def create(materializer: ActorBasedFlowMaterializer, flowName: String): (Publisher[ByteString], Unit) =
        if (!getAndSet(true)) {
          val ref = materializer.actorOf(props(materializer), name = s"$flowName-0-InputStream-source")
          val publisher = ActorPublisher[ByteString](ref)
          ref ! ExposedPublisher(publisher.asInstanceOf[impl.ActorPublisher[Any]])

          (publisher, ())
        } else (ErrorPublisher(new IllegalStateException("One time source can only be instantiated once")).asInstanceOf[Publisher[ByteString]], ())
    }
  }

  /**
   * Returns a source that can only be used once for testing purposes.
   */
  def oneTimeSource[T](other: Source[T]): Source[T] = {
    import akka.stream.impl._
    val original = other.asInstanceOf[ActorFlowSource[T]]
    new AtomicBoolean(false) with SimpleActorFlowSource[T] {
      override def attach(flowSubscriber: Subscriber[T], materializer: ActorBasedFlowMaterializer, flowName: String): Unit =
        create(materializer, flowName)._1.subscribe(flowSubscriber)
      override def isActive: Boolean = true
      override def create(materializer: ActorBasedFlowMaterializer, flowName: String): (Publisher[T], Unit) =
        if (!getAndSet(true)) (original.create(materializer, flowName)._1, ())
        else (ErrorPublisher(new IllegalStateException("One time source can only be instantiated once")).asInstanceOf[Publisher[T]], ())
    }
  }

  def runStrict(sourceData: ByteString, transformer: Flow[ByteString, ByteString]): Try[Option[ByteString]] =
    Try {
      transformer match {
        case Pipe(ops) ⇒
          if (ops.isEmpty)
            Some(sourceData)
          else {
            val buf = new ListBuffer[DeterministicOp[ByteString, ByteString]]
            @tailrec def trybuildStrictOps(remaining: List[AstNode]): List[DeterministicOp[ByteString, ByteString]] =
              remaining match {
                case Nil ⇒ buf.toList
                case OpFactory(mkOp :: Nil, _) :: tail ⇒
                  mkOp() match {
                    case d: DeterministicOp[ByteString, ByteString] ⇒
                      buf.append(d)
                      trybuildStrictOps(tail)
                    case _ ⇒ Nil
                  }
                case _ ⇒ Nil
              }

            val strictOps = trybuildStrictOps(ops)
            if (strictOps.isEmpty)
              None
            else {
              val iter = new IteratorInterpreter(Iterator.single(sourceData), strictOps).iterator
              var result = ByteString.empty
              // note that iter.next() will throw exception if the stream fails, caught by the enclosing Try
              while (iter.hasNext)
                result ++= iter.next()
              Some(result)
            }
          }

        case _ ⇒ None
      }
    }

}

/**
 * INTERNAL API
 */
private[http] class EnhancedByteStringSource(val byteStringStream: Source[ByteString]) extends AnyVal {
  def join(implicit materializer: FlowMaterializer): Future[ByteString] =
    byteStringStream.fold(ByteString.empty)(_ ++ _)
  def utf8String(implicit materializer: FlowMaterializer, ec: ExecutionContext): Future[String] =
    join.map(_.utf8String)
}
