package com.spingo.op_rabbit

import akka.actor._
import com.spingo.op_rabbit.RabbitExceptionMatchers._
import com.thenewmotion.akka.rabbitmq.{Channel, ChannelCreated, CreateChannel, ChannelActor}
import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration._

private [op_rabbit] class SubscriptionActor(subscription: Subscription[Consumer], connection: ActorRef) extends LoggingFSM[SubscriptionActor.State, SubscriptionActor.ConnectionInfo] {
  import SubscriptionActor._

  import RabbitControl.{Pause, Run}

  startWith(Paused, ConnectionInfo(None, None))

  val consumer = context.actorOf(subscription.consumer.props(subscription.binding.queueName), "consumer")
  context.watch(consumer)
  subscription._consumerRef.success(consumer)

  private case class ChannelConnected(channel: Channel, channelActor: ActorRef)

  val consumerStoppedP = Promise[Unit]
  val channelActorP = Promise[ActorRef]

  when(Running) {
    case Event(ChannelConnected(channel, _), info) =>
      subscribe(channel) using info.copy(channel = Some(channel))
    case Event(Pause, _) =>
      consumer.tell(Consumer.Unsubscribe, sender)
      goto(Paused)
    case Event(Run, _) =>
      stay replying true
  }

  when(Paused) {
    case Event(ChannelConnected(channel, _), info) =>
      stay using info.copy(channel = Some(channel))
    case Event(Run, connection) =>
      connection.channel map (subscribe) getOrElse (goto(Running)) replying true
    case Event(Pause, _) =>
      stay replying true
  }

  whenUnhandled {
    case Event(ChannelCreated(channelActor_), _) =>
      channelActorP.success(channelActor_)
      stay

    case Event(Terminated(actor), _) if actor == consumer =>
      // our consumer stopped; time to shut ourself down
      consumerStoppedP.success(())
      stay

    case Event(e, s) =>
      log.error("received unhandled request {} in state {}/{}", e, stateName, s)
      stay

  }
  onTermination {
    case StopEvent(_, _, connectionInfo) =>
      subscription._closedP.trySuccess(())
      stop()
  }

  initialize()

  override def preStart: Unit = {
    import ExecutionContext.Implicits.global
    val system = context.system
    subscription._closingP.future.foreach { timeout =>
      consumer ! Consumer.Shutdown
      context.system.scheduler.scheduleOnce(timeout) {
        subscription.abort()
      }
    }
    subscription.aborting.onComplete { _ =>
      consumer ! Consumer.Abort
    }
    connection ! CreateChannel(ChannelActor.props({(channel: Channel, channelActor: ActorRef) =>
      log.info(s"Channel created; ${channel}")
      self ! ChannelConnected(channel, channelActor)
    }))

    for {
      _               <- subscription.closed
      channelActorRef <- channelActorP.future
      _               <- consumerStoppedP.future
    } system stop channelActorRef

    for {
      _ <- consumerStoppedP.future
      _ <- channelActorP.future
    } system stop self
  }

  def subscribe(channel: Channel) = {
    subscription.binding.bind(channel)
    subscription._initializedP.trySuccess(Unit)
    consumer ! Consumer.Subscribe(channel)
    goto(Running)
  }

  def unsubscribe = {
  }

}

private [op_rabbit] object SubscriptionActor {
  sealed trait State
  case object Paused extends State
  case object Running extends State
  case object Stopped extends State

  sealed trait Commands
  def props(subscription: Subscription[Consumer], connection: ActorRef): Props =
    Props(classOf[SubscriptionActor], subscription, connection)

  case object Shutdown extends Commands
  case class ConnectionInfo(channelActor: Option[ActorRef], channel: Option[Channel])
}
