package com.spingo.op_rabbit

import akka.actor._
import akka.pattern.ask
import akka.stream.ActorFlowMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.Timeout
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.spingo.op_rabbit.DefaultMarshalling.utf8StringMarshaller
import com.spingo.op_rabbit.helpers.{DeleteQueue, RabbitTestHelpers}
import com.spingo.scoped_fixtures.ScopedFixtures
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.Try

class RabbitSourceSpec extends FunSpec with ScopedFixtures with Matchers with RabbitTestHelpers {

  implicit val executionContext = ExecutionContext.global


  val queueName = ScopedFixture[String] { setter =>
    val name = s"test-queue-rabbit-control-${Math.random()}"
    deleteQueue(name)
    val r = setter(name)
    deleteQueue(name)
    r
  }

  trait RabbitFixtures {
    implicit val materializer = ActorFlowMaterializer()
    val exceptionReported = Promise[Boolean]
    implicit val errorReporting = new RabbitErrorLogging {
      def apply(name: String, message: String, exception: Throwable, consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
        exceptionReported.success(true)
      }
    }
    val range = (0 to 16) toList
    val qos = 8
    lazy val binding = new QueueBinding(queueName(), durable = true, exclusive = false, autoDelete = false)
    lazy val subscription = Subscription(
      binding,
      RabbitSource[Int](
        name = "very-stream",
        qos = qos))
    rabbitControl ! subscription
  }

  describe("streaming from rabbitMq") {
    it("subscribes to a stream of events and shuts down sanely") {
      new RabbitFixtures {
        lazy val promises = range map (_ => Promise[Unit])
        val result = Source(subscription.consumer)
          .map { case (p, i) => (p, i) }
          .runWith(Sink.fold(0) {
            case (r, (p, i)) =>
              p.success() // ack the message in rabbitMq
              promises(i).success() // let our test know that this message was handled
              r + i
          })

        await(subscription.initialized)
        (range) foreach { i => rabbitControl ! QueueMessage(i, queueName()) }
        await(Future.sequence(promises.map(_.future)))
        // test is done; let's stop the stream
        subscription.close()

        await(result) should be (136)
        await(subscription.closed) // assert that subscription gets closed
      }
    }

    it("yields serialization exceptions through the stream and shuts down the subscription") {
      new RabbitFixtures {

        val result =
          Source(subscription.consumer)
          .runWith(Sink.foreach {
            case (p, i) =>
              p.success() // ack the message in rabbitMq
          })

        await(subscription.initialized)
        (range) foreach { i => rabbitControl ! QueueMessage("a", queueName()) }
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

          val result = Source(subscription.consumer).
            mapAsync(qos) { case (p, i) =>
              delivered(i).trySuccess(())
              floodgate.future map { _ => (p, i) }
            }.
            runWith(Sink.foreach {
              case (p, i) =>
                p.success() // ack the message in rabbitMq
            })

          await(subscription.initialized)
          (range) foreach { i => rabbitControl ! QueueMessage(i, queueName()) }
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
          override lazy val binding = QueueBinding(queueName(), durable = false, exclusive = false, autoDelete = true)
          val promises = range map { i => Promise[Unit]}
          val result = Source(subscription.consumer).
            runWith(Sink.foreach {
              case (p, i) =>
                promises(i).success()
                p.success() // ack the message in rabbitMq
            })
          await(subscription.initialized)
          (range) foreach { i => rabbitControl ! QueueMessage(i, queueName()) }
          promises.foreach(p => await(p.future))
          subscription.close()
          await(result)
        }
      }
    }
  }
}
