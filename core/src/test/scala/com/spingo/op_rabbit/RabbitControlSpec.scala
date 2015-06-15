package com.spingo.op_rabbit

import akka.actor._
import akka.pattern.ask
import com.spingo.scoped_fixtures.ScopedFixtures
import com.thenewmotion.akka.rabbitmq.{ChannelActor, ChannelMessage, RichConnectionActor}
import helpers.RabbitTestHelpers
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._

class RabbitControlSpec extends FunSpec with ScopedFixtures with Matchers with RabbitTestHelpers {

  trait RabbitFixtures {
    val queueName = s"test-queue-rabbit-control"
    implicit val executionContext = ExecutionContext.global
    val rabbitControl = rabbitControlFixture()
  }

  describe("pausing subscriptions") {
    it("unsubscribes all subscriptions") {
      new RabbitFixtures {
        import RabbitControl._
        var count = 0
        val promises = (0 to 2) map { i => Promise[Int] } toList
        val consumer = AsyncAckingConsumer[Int]("Test", 10 seconds, qos = 5) { i =>
          Future {
            println(s"received $i")
            count += 1
            promises(i).success(i)
          }
        }
        val subscription = Subscription(
          QueueBinding(
            queueName,
            durable = false,
            exclusive = false),
          consumer)

        rabbitControl ! subscription
        await(subscription.initialized)

        rabbitControl ! QueueMessage(0, queueName)
        await(promises(0).future)
        count shouldBe 1

        await(rabbitControl ? Pause)
        rabbitControl ! QueueMessage(1, queueName)
        Thread.sleep(500) // TODO what to use instead of sleep?
        count shouldBe 1 // unsubscribed, no new messages processed

        await(rabbitControl ? Run)
        rabbitControl ! QueueMessage(2, queueName)
        await(Future.sequence(Seq(promises(1).future, promises(2).future)))
        count shouldBe 3 // resubscribed, all messages processed

        // clean up rabbit queue
        val connectionActor = await(rabbitControl ? GetConnectionActor).asInstanceOf[ActorRef]
        val channel = connectionActor.createChannel(ChannelActor.props())
        deleteQueue(queueName)
      }
    }
  }

  describe("ConfirmedMessage publication") {
    it("fulfills the published promise on delivery confirmation") {
      new RabbitFixtures {
        val consumer = AsyncAckingConsumer[Int]("Test", 10 seconds, qos = 5) { _ =>
          Future.successful(Unit)
        }
        val subscription = Subscription(
          QueueBinding(
            queueName,
            durable = false,
            exclusive = false),
          consumer)
        rabbitControl ! subscription
        await(subscription.initialized)

        val msg = ConfirmedMessage(QueuePublisher(queueName), 5)
        rabbitControl ! msg

        await(msg.published)
        deleteQueue(queueName)
      }
    }

    // TODO - make this test not suck
    it("handles connection interruption without dropping messages") {
      new RabbitFixtures {
        var received = List.empty[Int]
        var countConfirmed = 0
        var countReceived = 0
        var lastReceived = -1
        val doneConfirm = Promise[Unit]
        val doneReceive = Promise[Unit]

        val counter = actorSystem.actorOf(Props(new Actor {
          def receive = {
            case ('confirm, -1) =>
              doneConfirm.success()
            case ('receive, -1) =>
              doneReceive.success()
            case ('confirm, n: Int) =>
              println(s"== confirm $n")
              countConfirmed += 1
            case ('receive, n: Int) =>
              println(s"receive $n")
              if(n <= lastReceived) // duplicate message
                ()
              else {
                countReceived += 1
                lastReceived = n
              }
          }
        }))

        val consumer = AsyncAckingConsumer[Int]("Test", 10 seconds, qos = 5) { i =>
          counter ! ('receive, i)
          Future.successful(Unit)
        }
        val subscription = Subscription(
          QueueBinding(
            queueName,
            durable = true,
            exclusive = false),
          consumer)
        rabbitControl ! subscription
        await(subscription.initialized)

        val factory = ConfirmedMessage.factory(QueuePublisher(queueName))

        var keepSending = true
        val lastSentF = Future {
          var i = 0
          while (keepSending) {
            i = i + 1
            val n = i
            val msg = factory(n)
            msg.published foreach { _ =>
              counter ! ('confirm, n)
            }
            rabbitControl ! msg
            Thread.sleep(10) // slight delay as to not overwhelm RAM
          }
          i
        }

        Thread.sleep(100)
        reconnect(rabbitControl)
        keepSending = false
        val lastSent = await(lastSentF)
        val confirmMsg = factory(-1)
        rabbitControl ! confirmMsg
        confirmMsg.published foreach { _ =>
          counter ! ('confirm, -1)
        }
        await(doneReceive.future)
        await(doneConfirm.future)
        println(lastSent)
        countReceived should be (countConfirmed)

        deleteQueue(queueName)
      }
    }
  }
}
