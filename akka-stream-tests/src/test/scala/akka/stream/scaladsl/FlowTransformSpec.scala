/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import akka.stream.FlowMaterializer
import akka.stream.MaterializerSettings
import akka.stream.testkit.{ AkkaSpec, StreamTestKit }
import akka.testkit.{ EventFilter, TestProbe }
import com.typesafe.config.ConfigFactory
import akka.stream.impl.fusing.Context
import akka.stream.impl.fusing.DeterministicOp
import akka.stream.impl.fusing.Directive
import akka.stream.impl.fusing.TerminationDirective
import akka.stream.impl.fusing.TransitivePullOp
import akka.stream.impl.fusing.RichDeterministicOp

class FlowTransformSpec extends AkkaSpec(ConfigFactory.parseString("akka.actor.debug.receive=off\nakka.loglevel=INFO")) {

  val settings = MaterializerSettings(system)
    .withInputBuffer(initialSize = 2, maxSize = 2)
    .withFanOutBuffer(initialSize = 2, maxSize = 2)

  implicit val materializer = FlowMaterializer(settings)

  "A Flow with transform operations" must {
    "produce one-to-one transformation as expected" in {
      val p = Source(List(1, 2, 3)).runWith(Sink.publisher)
      val p2 = Source(p).
        transform("transform", () ⇒ new TransitivePullOp[Int, Int] {
          var tot = 0
          override def onPush(elem: Int, ctxt: Context[Int]) = {
            tot += elem
            ctxt.push(tot)
          }
        }).
        runWith(Sink.publisher)
      val subscriber = StreamTestKit.SubscriberProbe[Int]()
      p2.subscribe(subscriber)
      val subscription = subscriber.expectSubscription()
      subscription.request(1)
      subscriber.expectNext(1)
      subscriber.expectNoMsg(200.millis)
      subscription.request(2)
      subscriber.expectNext(3)
      subscriber.expectNext(6)
      subscriber.expectComplete()
    }

    "produce one-to-several transformation as expected" in {
      val p = Source(List(1, 2, 3)).runWith(Sink.publisher)
      val p2 = Source(p).
        transform("transform", () ⇒ new RichDeterministicOp[Int, Int] {
          var tot = 0

          lazy val waitForNext = new State {
            override def onPush(elem: Int, ctxt: Context[Int]) = {
              tot += elem
              val iter = Vector.fill(elem)(tot).iterator
              emit(Vector.fill(elem)(tot).iterator, ctxt)
            }
          }

          override def initial = waitForNext

          override def onUpstreamFinish(ctxt: Ctxt): TerminationDirective = {
            if (current eq waitForNext) ctxt.finish()
            else ctxt.absorbTermination()
          }

        }).
        runWith(Sink.publisher)
      val subscriber = StreamTestKit.SubscriberProbe[Int]()
      p2.subscribe(subscriber)
      val subscription = subscriber.expectSubscription()
      subscription.request(4)
      subscriber.expectNext(1)
      subscriber.expectNext(3)
      subscriber.expectNext(3)
      subscriber.expectNext(6)
      subscriber.expectNoMsg(200.millis)
      subscription.request(100)
      subscriber.expectNext(6)
      subscriber.expectNext(6)
      subscriber.expectComplete()
    }

    "produce dropping transformation as expected" in {
      val p = Source(List(1, 2, 3, 4)).runWith(Sink.publisher)
      val p2 = Source(p).
        transform("transform", () ⇒ new TransitivePullOp[Int, Int] {
          var tot = 0
          override def onPush(elem: Int, ctxt: Context[Int]) = {
            tot += elem
            if (elem % 2 == 0)
              ctxt.pull()
            else
              ctxt.push(tot)
          }
        }).
        runWith(Sink.publisher)
      val subscriber = StreamTestKit.SubscriberProbe[Int]()
      p2.subscribe(subscriber)
      val subscription = subscriber.expectSubscription()
      subscription.request(1)
      subscriber.expectNext(1)
      subscriber.expectNoMsg(200.millis)
      subscription.request(1)
      subscriber.expectNext(6)
      subscription.request(1)
      subscriber.expectComplete()
    }

    "produce multi-step transformation as expected" in {
      val p = Source(List("a", "bc", "def")).runWith(Sink.publisher)
      val p2 = Source(p).
        transform("transform", () ⇒ new TransitivePullOp[String, Int] {
          var concat = ""
          override def onPush(elem: String, ctxt: Context[Int]) = {
            concat += elem
            ctxt.push(concat.length)
          }
        }).
        transform("transform", () ⇒ new TransitivePullOp[Int, Int] {
          var tot = 0
          override def onPush(length: Int, ctxt: Context[Int]) = {
            tot += length
            ctxt.push(tot)
          }
        }).
        runWith(Sink.fanoutPublisher(2, 2))
      val c1 = StreamTestKit.SubscriberProbe[Int]()
      p2.subscribe(c1)
      val sub1 = c1.expectSubscription()
      val c2 = StreamTestKit.SubscriberProbe[Int]()
      p2.subscribe(c2)
      val sub2 = c2.expectSubscription()
      sub1.request(1)
      sub2.request(2)
      c1.expectNext(1)
      c2.expectNext(1)
      c2.expectNext(4)
      c1.expectNoMsg(200.millis)
      sub1.request(2)
      sub2.request(2)
      c1.expectNext(4)
      c1.expectNext(10)
      c2.expectNext(10)
      c1.expectComplete()
      c2.expectComplete()
    }

    "support emit onUpstreamFinish" in {
      val p = Source(List("a")).runWith(Sink.publisher)
      val p2 = Source(p).
        transform("transform", () ⇒ new RichDeterministicOp[String, String] {
          var s = ""
          override def initial = new State {
            override def onPush(element: String, ctxt: Context[String]) = {
              s += element
              ctxt.pull()
            }
          }
          override def onUpstreamFinish(ctxt: Context[String]) =
            terminationEmit(Iterator.single(s + "B"), ctxt)
        }).
        runWith(Sink.publisher)
      val c = StreamTestKit.SubscriberProbe[String]()
      p2.subscribe(c)
      val s = c.expectSubscription()
      s.request(1)
      c.expectNext("aB")
      c.expectComplete()
    }

    "allow early finish" in {
      val p = StreamTestKit.PublisherProbe[Int]()
      val p2 = Source(p).
        transform("transform", () ⇒ new TransitivePullOp[Int, Int] {
          var s = ""
          override def onPush(element: Int, ctxt: Context[Int]) = {
            s += element
            if (s == "1")
              ctxt.pushAndFinish(element)
            else
              ctxt.push(element)
          }
        }).
        runWith(Sink.publisher)
      val proc = p.expectSubscription
      val c = StreamTestKit.SubscriberProbe[Int]()
      p2.subscribe(c)
      val s = c.expectSubscription()
      s.request(10)
      proc.sendNext(1)
      proc.sendNext(2)
      c.expectNext(1)
      c.expectComplete()
      proc.expectCancellation()
    }

    "report error when exception is thrown" in {
      val p = Source(List(1, 2, 3)).runWith(Sink.publisher)
      val p2 = Source(p).
        transform("transform", () ⇒ new RichDeterministicOp[Int, Int] {
          override def initial = new State {
            override def onPush(elem: Int, ctxt: Context[Int]) = {
              if (elem == 2) {
                throw new IllegalArgumentException("two not allowed")
              } else {
                emit(Iterator(elem, elem), ctxt)
              }
            }
          }
        }).
        runWith(Sink.publisher)
      val subscriber = StreamTestKit.SubscriberProbe[Int]()
      p2.subscribe(subscriber)
      val subscription = subscriber.expectSubscription()
      EventFilter[IllegalArgumentException]("two not allowed") intercept {
        subscription.request(100)
        subscriber.expectNext(1)
        subscriber.expectNext(1)
        subscriber.expectError().getMessage should be("two not allowed")
        subscriber.expectNoMsg(200.millis)
      }
    }

    "support cancel as expected" in {
      val p = Source(List(1, 2, 3)).runWith(Sink.publisher)
      val p2 = Source(p).
        transform("transform", () ⇒ new RichDeterministicOp[Int, Int] {
          override def initial = new State {
            override def onPush(elem: Int, ctxt: Context[Int]) =
              emit(Iterator(elem, elem), ctxt)
          }
        }).
        runWith(Sink.publisher)
      val subscriber = StreamTestKit.SubscriberProbe[Int]()
      p2.subscribe(subscriber)
      val subscription = subscriber.expectSubscription()
      subscription.request(2)
      subscriber.expectNext(1)
      subscription.cancel()
      subscriber.expectNext(1)
      subscriber.expectNoMsg(500.millis)
      subscription.request(2)
      subscriber.expectNoMsg(200.millis)
    }

    "support producing elements from empty inputs" in {
      val p = Source(List.empty[Int]).runWith(Sink.publisher)
      val p2 = Source(p).
        transform("transform", () ⇒ new RichDeterministicOp[Int, Int] {
          override def initial = new State {
            override def onPush(elem: Int, ctxt: Context[Int]) = ctxt.pull()
          }
          override def onUpstreamFinish(ctxt: Context[Int]) =
            terminationEmit(Iterator(1, 2, 3), ctxt)
        }).
        runWith(Sink.publisher)
      val subscriber = StreamTestKit.SubscriberProbe[Int]()
      p2.subscribe(subscriber)
      val subscription = subscriber.expectSubscription()
      subscription.request(4)
      subscriber.expectNext(1)
      subscriber.expectNext(2)
      subscriber.expectNext(3)
      subscriber.expectComplete()

    }

    "support converting onComplete into onError" in {
      val subscriber = StreamTestKit.SubscriberProbe[Int]()
      Source(List(5, 1, 2, 3)).transform("transform", () ⇒ new TransitivePullOp[Int, Int] {
        var expectedNumberOfElements: Option[Int] = None
        var count = 0
        override def onPush(elem: Int, ctxt: Context[Int]) =
          if (expectedNumberOfElements.isEmpty) {
            expectedNumberOfElements = Some(elem)
            ctxt.pull()
          } else {
            count += 1
            ctxt.push(elem)
          }

        override def onUpstreamFinish(ctxt: Context[Int]) =
          expectedNumberOfElements match {
            case Some(expected) if count != expected ⇒
              throw new RuntimeException(s"Expected $expected, got $count") with NoStackTrace
            case _ ⇒ ctxt.finish()
          }
      }).to(Sink(subscriber)).run()

      val subscription = subscriber.expectSubscription()
      subscription.request(10)

      subscriber.expectNext(1)
      subscriber.expectNext(2)
      subscriber.expectNext(3)
      subscriber.expectError().getMessage should be("Expected 5, got 3")
    }

    "be safe to reuse" in {
      val flow = Source(1 to 3).transform("transform", () ⇒
        new TransitivePullOp[Int, Int] {
          var count = 0

          override def onPush(elem: Int, ctxt: Context[Int]) = {
            count += 1
            ctxt.push(count)
          }
        })

      val s1 = StreamTestKit.SubscriberProbe[Int]()
      flow.to(Sink(s1)).run()
      s1.expectSubscription().request(3)
      s1.expectNext(1, 2, 3)
      s1.expectComplete()

      val s2 = StreamTestKit.SubscriberProbe[Int]()
      flow.to(Sink(s2)).run()
      s2.expectSubscription().request(3)
      s2.expectNext(1, 2, 3)
      s2.expectComplete()
    }
  }

}
