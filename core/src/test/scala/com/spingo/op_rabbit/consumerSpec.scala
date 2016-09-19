package com.spingo.op_rabbit

import akka.actor._
import com.rabbitmq.client.{Channel, Envelope}
import com.rabbitmq.client.AMQP.BasicProperties
import com.spingo.scoped_fixtures.ScopedFixtures
import com.spingo.op_rabbit.helpers.RabbitTestHelpers
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Random,Try,Failure}
import java.util.concurrent.atomic.AtomicInteger

class ConsumerSpec extends FunSpec with ScopedFixtures with Matchers with RabbitTestHelpers {

  val _queueName = ScopedFixture[String] { setter =>
    val name = s"test-queue-rabbit-control-${Math.random()}"
    deleteQueue(name)
    val r = setter(name)
    deleteQueue(name)
    r
  }
  implicit val executionContext = ExecutionContext.global
  trait RabbitFixtures {
    // import DefaultMarshalling._
    val queueName = _queueName()
  }

  describe("concurrent subscriptions") {
    it("properly handles multiple subscriptions at a time") {
      pending
    }
  }
  describe("consuming messages asynchronously") {
    it("receives and acks every message") {
      implicit val recoveryStrategy = RecoveryStrategy.limitedRedeliver()
      new RabbitFixtures {
        import RabbitErrorLogging.defaultLogger

        Future { 3 }


        val range = (0 to 100)
        val promises = range.map { _ => Promise[Int] }.toList
        val generator = new Random(123);
        val subscription = Subscription.run(rabbitControl) {
          import Directives._
          channel() {
            consume(queue(
              queueName,
              durable    = false,
              exclusive  = false,
              autoDelete = true)) {
              body(as[Int]) { i =>
                println(s"Received #${i}")
                Thread.sleep(Math.round(generator.nextDouble() * 100))
                promises(i).success(i)
                ack
              }
            }
          }
        }

        Await.result(subscription.initialized, 10.seconds)
        range foreach { i =>
          rabbitControl ! Message.queue(i, queueName)
        }
        val results = Await.result(Future.sequence(promises map (_.future)), 5.minutes)
        results shouldBe (range.toList)
      }
    }
  }

  describe("RecoveryStrategy none") {
    it("shuts down the subscription and passes the exception through subscriptionRef.closed") {
      new RabbitFixtures {
        implicit val recoveryStrategy = RecoveryStrategy.none
        case object LeError extends Exception("le error")

        val subscription = Subscription.run(rabbitControl) {
          import Directives._

          channel(1) {
            consume(queue(queueName)) {
              body(as[Int]) { b =>
                throw LeError
              }
            }
          }
        }

        Await.result(subscription.initialized, 10.seconds)
        rabbitControl ! Message.queue(1, queueName)

        Try(Await.result(subscription.closed, 10.seconds)) should be (Failure(LeError))
      }
    }
  }

  describe("RecoveryStrategy limitedRedeliver") {
    trait RedeliveryFixtures {
      var errors = 0
      implicit val logging = new RabbitErrorLogging {
        def apply(name: String, message: String, exception: Throwable, consumerTag: String, envelope: Envelope,
          properties: BasicProperties, body: Array[Byte]): Unit = {
          println(s"ERROR ${bodyAsString(body, properties)}")
          errors += 1
        }
      }
      val range = (0 to 9)
      val seen = range.map { _ => new AtomicInteger() }.toList
      lazy val promises = range.map { i => Stream.continually(Promise[Int]).take(retryCount + 1).toVector }.toList
      val retryCount: Int
      val queueName: String
      def awaitDeliveries() = Await.result(
        Future.sequence(promises.flatten.map(_.future)), 10.seconds)
      val directExchange = Exchange.direct("amq.direct", durable = false, autoDelete = true)
      def countAndRejectSubscription()(implicit recoveryStrategy: RecoveryStrategy) =
        Subscription {
          import Directives._

          val directBinding =
            Binding.direct(
              queue(queueName, durable = false, exclusive = false, autoDelete = true),
              Exchange.passive(directExchange))

          channel(qos = 3) {
            consume(directBinding) {
              body(as[Int]) { i =>
                promises(i)(seen(i).getAndIncrement()).success(i)
                ack(Future.failed(new Exception("Such failure")))
              }
            }
          }
        }
    }

    it("attempts every message twice when retryCount = 1") {
      new RedeliveryFixtures with RabbitFixtures {
        val retryCount = 1

        implicit val recoveryStrategy = RecoveryStrategy.limitedRedeliver(redeliverDelay = 100.millis, retryCount = 1)

        val subscriptionDef = countAndRejectSubscription()

        val subscription = subscriptionDef.run(rabbitControl)
        Await.result(subscription.initialized, 10.seconds)
        range foreach { i => rabbitControl ! Message.queue(i, queueName) }
        awaitDeliveries()
        Thread.sleep(1000) // give it time to finish rejecting messages
        (seen map (_.get)).distinct should be (List(2))
        errors should be (20)
      }
    }

    describe("onAbandon") {
      it("applies the onAbandon recoveryStrategy, preserving original exchange and routing key") {
        new RedeliveryFixtures {
          val queueName          = "redeliveryFailedQueueTest"
          val abandonedQueueName = s"op-rabbit.abandoned.${queueName}"
          val retryQueueName     = s"op-rabbit.retry.${queueName}"
          val retryCount         = 2

          try {
            implicit val recoveryStrategy = RecoveryStrategy.limitedRedeliver(
              redeliverDelay = 100.millis,
              retryCount     = retryCount,
              onAbandon      = RecoveryStrategy.abandonedQueue(3.seconds))
            val subscription = countAndRejectSubscription()(recoveryStrategy).run(rabbitControl)
            await(subscription.initialized)

            range foreach { i =>
              rabbitControl ! Message.exchange(i, directExchange.exchangeName, routingKey = queueName)
            }
            awaitDeliveries()

            subscription.close()
            await(subscription.closed)

            val nineReceived = Promise[(String, String)]
            val errorSubscription = Subscription.run(rabbitControl) {
              import Directives._
              channel(1) {
                consume(pqueue(abandonedQueueName)) {
                  (body(as[Int]) &
                    property(RecoveryStrategy.`x-original-exchange`) &
                    property(RecoveryStrategy.`x-original-routing-key`)) { (i, rk, x) =>
                    if (i == 9) nineReceived.success((rk, x))
                    ack
                  }
                }
              }
            }
            await(errorSubscription.initialized)
            await(nineReceived.future) shouldBe (("amq.direct", queueName))
          } finally {
            deleteQueue(queueName)
          }
        }
      }

      it("passively accepts the previous ttl configuration") {
        new RedeliveryFixtures {
          val queueName = "redeliveryFailedQueueTest"
          val retryCount = 0

          try {
            (List(3.seconds, 4.seconds)) foreach { time =>
              implicit val recoveryStrategy = RecoveryStrategy.limitedRedeliver(
                redeliverDelay = 100.millis,
                retryCount     = 1,
                onAbandon      = RecoveryStrategy.abandonedQueue(time))
              val subscription = countAndRejectSubscription()

              val subscriptionRef = subscription.run(rabbitControl)
              Await.result(subscriptionRef.initialized, 10.seconds)
              range foreach { i => rabbitControl ! Message.queue(i, queueName) }
              awaitDeliveries()
              subscriptionRef.close()
              await(subscriptionRef.closed)
            }

          } finally {
            deleteQueue(queueName)
          }
        }
      }
    }
  }

  describe("shutting down") {

    it("waits until all pending promises are acked prior to closing the subscription") {
      new RabbitFixtures {
        implicit val recoveryStrategy = RecoveryStrategy.limitedRedeliver()
        val ackThem        = Promise[Unit]
        val range          = (0 to 15).toList
        val receivedCounts = scala.collection.mutable.IndexedSeq.fill(range.length)(0)
        val received       = range map { i => Promise[Unit] }
        val firstEight     = received.take(8)

        def getSubscription(n: Int) = Subscription.run(rabbitControl) {
          import Directives._
          channel(qos = 8) {
            consume(queue(queueName, durable = true, exclusive = false, autoDelete = false)) {
              body(as[Int]) { i =>
                receivedCounts(i) = receivedCounts(i) + 1
                received(i).success(())
                ackThem.future.map { _ => Thread.sleep(50 * i.toLong) }
                ack(())
              }
            }
          }
        }

        val subscription1 = getSubscription(1)
        await(subscription1.initialized)
        (range) foreach { i => rabbitControl ! Message.queue(i, queueName) }
        ackThem.future.foreach(_ => subscription1.close())
        await(Future.sequence(firstEight.map(_.future)))
        println("Round 1 complete")
        println(s"receivedCounts = ${receivedCounts}")
        subscription1.close()
        await(subscription1.closed)

        /* At this point, every message that has begun (the first 8, since qos = 8) should have been acked.  We'll test
        this by reconnecting to RabbitMQ (rabbit avoids redelivering messages to same connection), then consuming what
        remains.  Finally, we'll count how many times we received each message.  If we did our job, then we'll only have
        received every message one and only one time. */

        reconnect(rabbitControl)

        val subscription2 = getSubscription(2)
        println(s"--------------------------- waiting for the rest of the futures to be consumed")
        await(Future.sequence(received.map(_.future)))

        subscription2.close()
        await(subscription2.closed)

        println("Round 2 complete")
        println(s"receivedCounts = ${receivedCounts}")

        rabbitControl ! new MessageForPublicationLike {
          val dropIfNoChannel = false
          def apply(channel: Channel): Unit =
            channel.queueDelete(queueName)
        }

        receivedCounts should be (range map (_ => 1))
      }
    }

    it("does not wait for pending promises to be acked when aborting the subscription") {
      new RabbitFixtures {
        val ackThem = Promise[Unit]
        val range = (0 to 15).toList
        val receivedCounts = scala.collection.mutable.IndexedSeq.fill(range.length)(0)
        val received = range map { i => Promise[Unit] }
        val firstEight = received.take(8)
        implicit val recoveryStrategy = RecoveryStrategy.limitedRedeliver()

        val subscription = Subscription.run(rabbitControl) {
          import Directives._
          channel(qos = 8) {
            consume(queue(queueName, durable = true, exclusive = false, autoDelete = false)) {
              body(as[Int]) { i =>
                println(s"${i} received")
                receivedCounts(i) = receivedCounts(i) + 1
                received(i).success(())
                ackThem.future.map { _ => Thread.sleep(50 * i.toLong) }
                ack(())
              }
            }
          }
        }

        await(subscription.initialized)
        (range) foreach { i => rabbitControl ! Message.queue(i, queueName) }
        await(Future.sequence(firstEight.map(_.future)))
        println("Round 1 complete")
        println(s"receivedCounts = ${receivedCounts}")
        subscription.abort
        ackThem.completeWith(subscription.closed)
        await(subscription.closed) // the fact that we can get here is evidence that it works, since we don't even ack
                                   // the messages until the consumer is closed
      }
    }
  }

}
