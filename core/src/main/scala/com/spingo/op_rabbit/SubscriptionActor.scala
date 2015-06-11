package com.spingo.op_rabbit

import akka.actor._
import com.spingo.op_rabbit.RabbitExceptionMatchers._
import com.thenewmotion.akka.rabbitmq.{Connection, Channel, ChannelCreated, CreateChannel, ChannelActor}
import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration._

private [op_rabbit] class SubscriptionActor(subscription: Subscription[Consumer], connection: Connection) extends Actor {
  import SubscriptionActor.{Paused, Running}
  import RabbitControl.{Pause, Run}

  val consumer = context.actorOf(subscription.consumer.props(subscription.binding.queueName), "consumer")
  context.watch(consumer)
  subscription._consumerRef.success(consumer)
  val channel = connection.createChannel()

  override def preStart: Unit = {
    import ExecutionContext.Implicits.global

    subscription.binding.bind(channel)
    subscription._initializedP.trySuccess(Unit)

    val system = context.system
    // Signal the consumer to shutdown / abort when these channels are fulfilled
    subscription._closingP.future.foreach { timeout =>
      consumer ! Consumer.Shutdown
      context.system.scheduler.scheduleOnce(timeout) {
        subscription.abort()
      }
    }
    subscription.aborting.onComplete { _ =>
      consumer ! Consumer.Abort
    }
  }

  override def postStop: Unit = {
    subscription._closedP.success()
    channel.close()
  }

  val default: Receive = {
    case Terminated(actor) if actor == consumer =>
      context.stop(self)
  }

  val paused: Receive = default orElse {
    case Pause =>
      sender ! true

    case Run =>
      consumer.tell(Consumer.Subscribe(channel), sender)
      context.become(running)
  }

  val running: Receive = default orElse {
    case Pause =>
      consumer.tell(Consumer.Unsubscribe, sender)
      context.become(paused)
    case Run =>
      sender ! true
  }

  val receive = paused orElse default
}

private [op_rabbit] object SubscriptionActor {
  sealed trait State
  case object Paused extends State
  case object Running extends State

  sealed trait Commands
  def props(subscription: Subscription[Consumer], connection: Connection): Props =
    Props(classOf[SubscriptionActor], subscription, connection)

  case object Shutdown extends Commands
  case class ConnectionInfo(channel: Channel)
}
