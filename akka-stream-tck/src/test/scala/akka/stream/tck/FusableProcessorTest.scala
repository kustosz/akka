/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.tck

import java.util.concurrent.atomic.AtomicInteger
import akka.stream.impl.{ Ast, ActorBasedFlowMaterializer }
import akka.stream.{ FlowMaterializer, MaterializerSettings }
import org.reactivestreams.{ Publisher, Processor }
import akka.stream.impl.fusing.Map
import akka.stream.impl.fusing.Op
import akka.stream.impl.fusing.Context
import akka.stream.impl.fusing.Directive

class FusableProcessorTest extends AkkaIdentityProcessorVerification[Int] {

  val processorCounter = new AtomicInteger

  override def createIdentityProcessor(maxBufferSize: Int): Processor[Int, Int] = {
    val settings = MaterializerSettings(system)
      .withInputBuffer(initialSize = maxBufferSize / 2, maxSize = maxBufferSize)

    implicit val materializer = FlowMaterializer(settings)(system)

    val flowName = getClass.getSimpleName + "-" + processorCounter.incrementAndGet()

    def op(): Op[Int, Int] = Map[Int, Int](identity)

    val processor = materializer.asInstanceOf[ActorBasedFlowMaterializer].processorForNode(
      Ast.OpFactory(List(() ⇒ op(), () ⇒ op(), () ⇒ op()), "identity"), flowName, 1)

    processor.asInstanceOf[Processor[Int, Int]]
  }

  override def createHelperPublisher(elements: Long): Publisher[Int] = {
    implicit val mat = FlowMaterializer()(system)

    createSimpleIntPublisher(elements)(mat)
  }

}
