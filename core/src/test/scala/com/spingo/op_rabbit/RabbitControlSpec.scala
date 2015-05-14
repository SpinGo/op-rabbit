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
        channel ! ChannelMessage { channel =>
          channel.queueDelete(queueName)
        }
      }
    }
  }
}
