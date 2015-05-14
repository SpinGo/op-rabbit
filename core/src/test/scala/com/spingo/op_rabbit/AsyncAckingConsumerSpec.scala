package com.spingo.op_rabbit

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ShutdownSignalException
import com.spingo.scoped_fixtures.ScopedFixtures
import com.thenewmotion.akka.rabbitmq.AmqpShutdownSignal
import com.thenewmotion.akka.rabbitmq.ChannelActor
import com.thenewmotion.akka.rabbitmq.CreateChannel
import helpers.RabbitTestHelpers
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.Random

class AsyncAckingConsumerSpec extends FunSpec with ScopedFixtures with Matchers with RabbitTestHelpers {

  trait RabbitFixtures {
    // import DefaultMarshalling._
    implicit val executionContext = ExecutionContext.global
  }
  val queueName = ScopedFixture[String] { setter =>
    val name = s"test-queue-rabbit-control-${Math.random()}"
    deleteQueue(name)
    val r = setter(name)
    deleteQueue(name)
    r
  }

  describe("concurrent subscriptions") {
    it("properly handles multiple subscriptions at a time") {
      pending
    }
  }
  describe("consuming messages asynchronously") {
    it("receives and acks every message") {
      new RabbitFixtures {
        import RabbitErrorLogging.defaultLogger

        val range = (0 to 100)
        val promises = range map { i => Promise[Int] } toList

        val generator = new Random(123);
        val consumer = AsyncAckingConsumer[Int]("Test", 10 seconds, qos = 5) { i =>
          Future {
            println(s"Received #${i}")
            Thread.sleep(Math.round(generator.nextDouble() * 100))
            promises(i).success(i)
          }
        }
        val subscription = new Subscription(
          QueueBinding(
            queueName(),
            durable = false,
            exclusive = false,
            autoDelete = true),
          consumer)

        rabbitControl ! subscription
        Await.result(subscription.initialized, 10 seconds)
        range foreach { i =>
          rabbitControl ! QueueMessage(i, queueName())
        }
        val results = Await.result(Future.sequence(promises map (_.future)), 5 minutes)
        results should be(range toList)
      }
    }
  }

  describe("error handling") {
    it("attempts every message twice") {
      new RabbitFixtures {
        var errors = 0
        implicit val logging = new RabbitErrorLogging {
          def apply(name: String, message: String, exception: Throwable, consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
            println(s"ERROR ${bodyAsString(body, properties)}")
            errors += 1
          }
        }

        val range = (0 to 9)
        case class Counter(var i: Int = 0) { def ++ = { i+=1; i-1}}
        val seen = range map { _ => Counter(0) } toList
        val promises = range map { i => List(Promise[Int], Promise[Int]) } toList
        val consumer = AsyncAckingConsumer[Int]("Test", 100 millis, retryCount = 1, qos = 3) { i =>
          promises(i)(seen(i)++).success(i)
          Future.failed(new Exception("Such failure"))
        }
        val subscription = new Subscription(
          QueueBinding(
            queueName(),
            durable = false,
            exclusive = false,
            autoDelete = true),
          consumer)

        rabbitControl ! subscription
        Await.result(subscription.initialized, 10 seconds)
        range foreach { i => rabbitControl ! QueueMessage(i, queueName()) }
        val results = Await.result(Future.sequence(promises.flatten map (_.future)), 10 seconds)
        Thread.sleep(1000) // give it time to finish rejecting messages
        (seen map (_.i)).distinct should be (List(2))
        errors should be (20)
      }
    }
  }

  describe("shutting down") {

    it("waits until all pending promises are acked prior to closing the subscription") {
      new RabbitFixtures {
        val binding = QueueBinding(
          queueName(),
          durable = true,
          exclusive = false,
          autoDelete = false)
        val ackThem = Promise[Unit]
        val range = (0 to 15) toList
        val receivedCounts = scala.collection.mutable.IndexedSeq.fill(range.length)(0)
        val received = range map { i => Promise[Unit] }
        val firstEight = received.take(8)

        def consumer(n: Int) = AsyncAckingConsumer[Int](s"Consumer${n}", 100 millis, retryCount = 1, qos = 8) { i =>
          println(s"${i} received")
          receivedCounts(i) = receivedCounts(i) + 1
          received(i).success(())
          ackThem.future.map { _ => Thread.sleep(50 * i) }
        }

        val subscription1 = new Subscription(
          binding,
          consumer(1))

        rabbitControl ! subscription1
        await(subscription1.initialized)
        (range) foreach { i => rabbitControl ! QueueMessage(i, queueName()) }
        ackThem.completeWith(subscription1.closing)
        await(Future.sequence(firstEight.map(_.future)))
        println("Round 1 complete")
        println(s"receivedCounts = ${receivedCounts}")
        subscription1.close()
        await(subscription1.closed)

        // at this point, every message that has begun (the first 8, since qos = 8) should have been acked
        // We'll test this by reconnecting to rabbitMq (rabbit avoids redelivering messages to same connection), then consuming what remains
        // Finally, we'll count how many times we received each message
        // If we did our job, then we'll only have received every message one and only one time.

        reconnect(rabbitControl)

        val subscription2 = new Subscription(
          binding,
          consumer(2))

        rabbitControl ! subscription2
        println(s"--------------------------- waiting for the rest of the futures to be consumed")
        await(Future.sequence(received.map(_.future)))

        subscription2.close()
        await(subscription2.closed)

        println("Round 2 complete")
        println(s"receivedCounts = ${receivedCounts}")

        rabbitControl ! new MessageForPublicationLike {
          val dropIfNoChannel = false
          def apply(channel: Channel): Unit =
            channel.queueDelete(queueName())
        }

        receivedCounts should be (range map (_ => 1))
      }
    }

    it("does not wait for pending promises to be acked when aborting the subscription") {
      new RabbitFixtures {
        val binding = QueueBinding(
          queueName(),
          durable = true,
          exclusive = false,
          autoDelete = false)
        val ackThem = Promise[Unit]
        val range = (0 to 15) toList
        val receivedCounts = scala.collection.mutable.IndexedSeq.fill(range.length)(0)
        val received = range map { i => Promise[Unit] }
        val firstEight = received.take(8)

        def consumer(n: Int) = AsyncAckingConsumer[Int](s"Consumer${n}", 100 millis, retryCount = 1, qos = 8) { i =>
          println(s"${i} received")
          receivedCounts(i) = receivedCounts(i) + 1
          received(i).success(())
          ackThem.future.map { _ => Thread.sleep(50 * i) }
        }

        val subscription1 = new Subscription(
          binding,
          consumer(1))

        rabbitControl ! subscription1
        await(subscription1.initialized)
        (range) foreach { i => rabbitControl ! QueueMessage(i, queueName()) }
        await(Future.sequence(firstEight.map(_.future)))
        println("Round 1 complete")
        println(s"receivedCounts = ${receivedCounts}")
        subscription1.abort
        ackThem.completeWith(subscription1.closed)
        await(subscription1.closed) // the fact that we can get here is evidence that it works, since we don't even ack the messages until the consumer is closed
      }
    }
  }

}
