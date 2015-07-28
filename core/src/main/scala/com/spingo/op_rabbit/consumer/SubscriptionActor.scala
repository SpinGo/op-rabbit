package com.spingo.op_rabbit.consumer

import akka.actor._
import com.rabbitmq.client.ShutdownSignalException
import com.spingo.op_rabbit.RabbitControl.{Pause, Run}
import com.spingo.op_rabbit.RabbitExceptionMatchers._
import com.thenewmotion.akka.rabbitmq.{Channel, ChannelActor, ChannelCreated, ChannelMessage, CreateChannel}
import java.io.IOException
import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration._


class SubscriptionActor(subscription: Subscription, connection: ActorRef) extends LoggingFSM[SubscriptionActor.State, SubscriptionActor.ConnectionInfo] {
  import SubscriptionActor._

  startWith(Paused, ConnectionInfo(None, subscription.channelConfiguration.qos))

  val props = Props {
    new impl.AsyncAckingRabbitConsumer(
      name             = subscription.binding.queueName,
      queueName        = subscription.binding.queueName,
      recoveryStrategy = subscription._recoveryStrategy,
      rabbitErrorLogging = subscription._errorReporting,
      handle           = subscription.handler)(subscription._executionContext)
  }

  val consumer = context.actorOf(props, "consumer")
  context.watch(consumer)
  subscription._subscriptionRef.success(self)

  private case class ChannelConnected(channel: Channel, channelActor: ActorRef)

  val consumerStoppedP = Promise[Unit]
  val channelActorP = Promise[ActorRef]

  when(Running) {
    case Event(ChannelConnected(_, _), info) =>
      subscribe(info) using info
    case Event(Pause, _) =>
      consumer.tell(Consumer.Unsubscribe, sender)
      goto(Paused)
    case Event(Run, _) =>
      stay replying true
  }

  when(Paused) {
    case Event(ChannelConnected(_, _), info) =>
      stay
    case Event(Run, connection) =>
      subscribe(connection) replying true
    case Event(Pause, _) =>
      stay replying true
  }

  whenUnhandled {
    case Event(ChannelCreated(channelActor_), info) =>
      channelActorP.success(channelActor_)
      stay using info.copy(channelActor = Some(channelActor_))

    case Event(Terminated(actor), _) if actor == consumer =>
      // our consumer stopped; time to shut ourself down
      consumerStoppedP.success(())
      stay

    case Event(Subscription.SetQos(qos), connectionInfo) =>
      connectionInfo.channelActor foreach { _ ! ChannelMessage { _.basicQos(qos) } }
      stay using connectionInfo.copy(qos = qos)

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

  // some errors close the channel and cause the error to come through async as the shutdown cause.
  def withShutdownCatching[T](channel:Channel)(fn: => T): Either[ShutdownSignalException, T] = {
    try {
      Right(fn)
    } catch {
      case e: IOException if ! channel.isOpen && ! channel.getCloseReason.isHardError => // hardError = true indicates connection issue, false = channel issue.
        Left(channel.getCloseReason())
    }
  }

  def subscribe(connectionInfo: ConnectionInfo) = {
    connectionInfo.channelActor foreach { channelActor =>
      channelActor ! ChannelMessage { channel =>
        channel.basicQos(connectionInfo.qos)

        withShutdownCatching(channel) {
          subscription.binding.bind(channel)
        } match {
          case Left(ex) =>
            subscription._initializedP.tryFailure(ex)
            subscription._closedP.tryFailure(ex) // propagate exception to closed future as well, as it's possible for the initialization to succeed at one point, but fail later.
            context stop self
          case Right(_) =>
            subscription._initializedP.trySuccess(Unit)
            consumer ! Consumer.Subscribe(channel)
        }
      }
    }
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
  def props(subscription: Subscription, connection: ActorRef): Props =
    Props(classOf[SubscriptionActor], subscription, connection)

  case object Shutdown extends Commands
  case class ConnectionInfo(channelActor: Option[ActorRef], qos: Int)
}
