package com.spingo.op_rabbit
package stream

import akka.actor._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.ActorMaterializerSettings
import akka.stream.Materializer
import akka.stream.Supervision
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.{Sink, Source}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.spingo.op_rabbit.Directives._
import com.spingo.op_rabbit.helpers.{DeleteQueue, RabbitTestHelpers}
import com.spingo.scoped_fixtures.ScopedFixtures
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Try,Success,Failure}

class RabbitSourceSpec extends FunSpec with ScopedFixtures with Matchers with RabbitTestHelpers {

  val queueName = ScopedFixture[String] { setter =>
    val name = s"test-queue-rabbit-control-${Math.random()}"
    deleteQueue(name)
    val r = setter(name)
    deleteQueue(name)
    r
  }

  trait RabbitFixtures {
    implicit val executionContext = ExecutionContext.global
    implicit val materializer = ActorMaterializer()
    val exceptionReported = Promise[Boolean]
    implicit val errorReporting = new RabbitErrorLogging {
      def apply(name: String, message: String, exception: Throwable, consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
        exceptionReported.trySuccess(true)
      }
    }
    val range = (0 to 16) toList
    val qos = 8

    implicit val recoveryStrategy = RecoveryStrategy.none
    lazy val binding = queue(queueName(), durable = true, exclusive = false, autoDelete = false)
    lazy val source = RabbitSource(
      rabbitControl,
      channel(qos = qos),
      consume(binding),
      body(as[Int]))
  }

  describe("streaming from rabbitMq") {
    it("subscribes to a stream of events and shuts down sanely") {
      new RabbitFixtures {
        lazy val promises = range map (_ => Promise[Unit])
        val (subscription, result) = source.
          map { i => promises(i).success(); i }.
          acked.
          toMat(Sink.fold(0)(_ + _))(Keep.both).run

        await(subscription.initialized)
        (range) foreach { i => rabbitControl ! Message.queue(i, queueName()) }
        await(Future.sequence(promises.map(_.future)))
        // test is done; let's stop the stream
        subscription.close()

        await(result) should be (136)
        await(subscription.closed) // assert that subscription gets closed
      }
    }

    it("yields serialization exceptions through the stream and shuts down the subscription") {
      new RabbitFixtures {

        val (subscription, result) = source.runAckMat(Keep.both)

        await(subscription.initialized)
        (range) foreach { i => rabbitControl ! Message.queue("a", queueName()) }
        // test is done; let's stop the stream

        a [java.lang.NumberFormatException] should be thrownBy {
          await(result)
        }

        await(subscription.closed) // assert that subscription gets closed
        await(exceptionReported.future) should be (true)
      }
    }

    it("recovers from connection interruptions by replaying unacked messages") {
      new RabbitFixtures {
        try {
          var delivered = range map { _ => Promise[Unit] }
          var floodgate = Promise[Unit]

          val (subscription, result) = source.
            mapAsync(qos) { i =>
              delivered(i).trySuccess(())
              floodgate.future map { _ => i }
            }.
            runAckMat(Keep.both)

          await(subscription.initialized)
          (range) foreach { i => rabbitControl ! Message.queue(i, queueName()) }
          delivered.take(8).foreach(p => await(p.future))
          await(Future.sequence( delivered.take(8).map(_.future)))
          // we should get up to 8 delivered

          delivered = range map { _ => Promise[Unit] }

          reconnect(rabbitControl)

          // open the floodgate; let's wait for the entire stream to complete
          floodgate.success()
          delivered.foreach(p => await(p.future)) // if this fails, it means it did not replay every message

          // test is done; let's stop the stream
          subscription.close()
          await(subscription.closed)
        } finally {
          println("deleting")
          val delete = DeleteQueue(queueName())
          rabbitControl ! DeleteQueue(queueName())
          Try {
            await(delete.processed, 1 second)
            println("deleted")
          }
        }
      }
    }

    it("handles being brought down, and started up again") {
      (0 to 1) foreach { time =>
        new RabbitFixtures {
          override lazy val binding = queue(queueName(), durable = false, exclusive = false, autoDelete = true)
          val promises = range map { i => Promise[Unit]}
          val (subscription, result) = source.
            map(promises(_).success(())).
            runAckMat(Keep.both)
          await(subscription.initialized)
          (range) foreach { i => rabbitControl ! Message.queue(i, queueName()) }
          promises.foreach(p => await(p.future))
          subscription.close()
          await(result)
        }
      }
    }
  }

}
