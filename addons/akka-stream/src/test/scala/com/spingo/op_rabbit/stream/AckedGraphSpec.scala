package com.spingo.op_rabbit.stream

import akka.actor._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.Graph
import akka.stream.Shape
import akka.stream.scaladsl.Broadcast
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.FlowGraph
import akka.stream.scaladsl.FlowGraph.Builder
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.spingo.op_rabbit.{ConfirmedMessage, QueuePublisher, RabbitControl}
import com.spingo.op_rabbit.consumer.{HListToValueOrTuple, RabbitErrorLogging}
import com.spingo.op_rabbit.consumer.Directives._
import com.spingo.op_rabbit.helpers.RabbitTestHelpers
import com.spingo.scoped_fixtures.ScopedFixtures
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import shapeless._
import akka.stream.scaladsl.Merge

class AckedGraphSpec extends FunSpec with ScopedFixtures with Matchers with RabbitTestHelpers {

  describe("thing") {
    it("normal") {
      case class LeException(msg: String) extends Exception(msg)
      implicit val materializer = ActorMaterializer()

      val source1 = AckedSource(1 to 5).unsafe.map { case (p, d) =>
        implicit val ec = SameThreadExecutionContext
        p.future.onComplete { t => println(s"Promise $d completed: ${t}") }
        (p,d)
      }
      // val s1 = Sink.foreach[AckTup[Int]](println)
      val s2 = Sink.foreach[AckTup[Int]](println)
      val Seq(s1, s3) = (1 to 2) map { n => AckedSink.foreach[Int](println).akkaGraph }

      val g = FlowGraph.closed(source1, s1, s2)((m1, m2, m3) => (m1, m2, m3)) { implicit b =>
        (source1, sink1, sink2) =>

        import FlowGraph.Implicits._
        val broadcast = b.add(AckedBroadcast[Int](2).akkaGraph)
        // val f = b.add(Flow[Int])
        source1 ~> broadcast ~> sink1
        broadcast ~> sink2

        // shape1 ~> merge ~> sink
        // shape2 ~> merge
      }

      val (r1, r2, r3) = g.run()
      await(r3)
      println(s"Very thing!")
    }

  }

  describe("AckedBroadcast") {
    it("broadcasts the values to each output") {
      val input = (Stream.continually(Promise[Unit]) zip Range.inclusive(1, 5)).toList
      implicit val materializer = ActorMaterializer()

      val source = AckedSource(input)

      val Seq(s1, s2) = (1 to 2) map { n => AckedSink.fold[Int, Int](0)(_ + _) }

      val g = AckedFlowGraph.closed(s1, s2)((m1, m2) => (m1, m2)) { implicit b =>
        (s1, s2) =>

        import AckedFlowGraph.Implicits._

        val broadcast = b.add(AckedBroadcast[Int](2))

        source ~> broadcast ~> s1
        broadcast ~> s2
      }

      // If AckedBroadcast failed to create a unique promise for each stream, then "java.lang.IllegalStateException: Promise already completed" would be thrown.
      val (f1, f2) = g.run()
      val (r1, r2) = (await(f1), await(f2))

      r1 should be (15)
      r2 should be (15)
      input.foreach { case (p, _) => await(p.future) }
    }
  }
}
