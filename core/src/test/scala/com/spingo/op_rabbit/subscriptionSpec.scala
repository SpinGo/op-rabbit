package com.spingo.op_rabbit

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.spingo.op_rabbit.subscription.Subscription
import com.spingo.scoped_fixtures.ScopedFixtures
import com.thenewmotion.akka.rabbitmq.{Channel, RichConnectionActor}
import helpers.RabbitTestHelpers
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._

class SubscriptionSpec extends FunSpec with ScopedFixtures with Matchers with RabbitTestHelpers {

  implicit val executionContext = ExecutionContext.global
  val queueName = s"test-queue-rabbit-control"

  trait RabbitFixtures {
  }

  describe("Allocating and releasing channels") {
    // it("allocates a channel on subscription, and closes it on shutdown") {
    //   new RabbitFixtures {

    //     val channelWatcher = new com.rabbitmq.client.ShutdownListener {
    //       private val _p = Promise[com.rabbitmq.client.ShutdownSignalException]
    //       val shutdown = _p.future
    //       def shutdownCompleted(cause: com.rabbitmq.client.ShutdownSignalException): Unit = {
    //         _p.success(cause)
    //       }
    //     }

    //     val binding = new Binding {
    //       val queueName = "test"
    //       var channel: Option[Channel] = None
    //       def bind(c: Channel): Unit = {
    //         channel = Some(c)
    //       }
    //     }

    //     val consumer = new Consumer {
    //       val name = "test"
    //       var channels = Seq.empty[Channel]
    //       var unsubscribes = Seq.empty[Int]
    //       val shutdownP = Promise[Unit]
    //       val subscribedP = Promise[Unit]
    //       var counter = 0

    //       def props(queueName: String) = Props {
    //         new Actor {
    //           def receive = {
    //             case Consumer.Unsubscribe =>
    //               unsubscribes = unsubscribes :+ counter
    //               sender ! true

    //             case Consumer.Subscribe(channel) =>
    //               subscribedP.success(Unit)
    //               channels = channels :+ channel
    //               counter += 1

    //             case Consumer.Shutdown =>
    //               shutdownP.success(Unit)
    //               context stop self
    //           }
    //         }
    //       }
    //     }

    //     val subscription = Subscription(binding, consumer)

    //     rabbitControl ! subscription
    //     await(subscription.initialized)
    //     await(consumer.subscribedP.future)

    //     binding.channel.nonEmpty should be(true)
    //     val channel = binding.channel.get
    //     channel.addShutdownListener(channelWatcher)
    //     consumer.channels should be (Seq(channel))

    //     // now test shutdown
    //     subscription.close()
    //     await(subscription.closed)

    //     // It closes the consumer
    //     await(consumer.shutdownP.future)

    //     // It closes the channel
    //     val shutdownReason = await(channelWatcher.shutdown).getReason
    //     shutdownReason.protocolMethodName should be ("channel.close")
    //     channel.isOpen should be (false) // close should have closed the channel associated with this subscription

    //     // Flush out the rest of the stuff
    //     actorSystem.shutdown()
    //     actorSystem.awaitTermination(timeout.duration)

    //     consumer.channels should be (Seq(channel)) // we shouldn't have received another channel after it was closed
    //     consumer.unsubscribes should be (Seq.empty)
    //   }
    // }
  }
}
