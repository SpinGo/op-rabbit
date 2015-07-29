package com.spingo.op_rabbit.consumer

import akka.actor._
import com.rabbitmq.client.ShutdownSignalException
import com.spingo.op_rabbit.RabbitControl.{Pause, Run}
import com.spingo.op_rabbit.RabbitExceptionMatchers._
import com.thenewmotion.akka.rabbitmq.{Channel, ChannelActor, ChannelCreated, ChannelMessage, CreateChannel}
import java.io.IOException
import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration._
import scala.util.{Try,Failure,Success}

class SubscriptionActor(subscription: Subscription, connection: ActorRef) extends LoggingFSM[SubscriptionActor.State, SubscriptionActor.SubscriptionPayload] {
  import SubscriptionActor._
  startWith(Paused, SubscriptionPayload(None, subscription.channelConfiguration.qos, false, None))

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

  when(Stopping) {
    // Because you can't monitor same-state transitions (A => A) in Akka 2.3.x, we send a Nudge message to self each time an action that would potentially cause the actor to be ready to be stopped.
    case Event(Nudge, payload) =>
      if (payload.consumerStopped && payload.channelActor.nonEmpty) {
        context stop self
        goto(Stopped)
      } else stay
  }

  whenUnhandled {
    case Event(ChannelCreated(channelActor_), info) =>
      stay using info.copy(channelActor = Some(channelActor_))

    case Event(Nudge, _) =>
      stay

    case Event(Subscription.SetQos(qos), connectionInfo) =>
      connectionInfo.channelActor foreach { _ ! ChannelMessage { _.basicQos(qos) } }
      stay using connectionInfo.copy(qos = qos)

    case Event(Terminated(actor), payload) if actor == consumer =>
      println(s"TERMINATED")
      self ! Nudge
      goto(Stopping) using payload.copy(consumerStopped = true)

    case Event(Stop(cause, timeout), connectionInfo) =>
      consumer ! Consumer.Shutdown
      import context.dispatcher
      context.system.scheduler.scheduleOnce(timeout, self, Abort(cause))
      self ! Nudge
      goto(Stopping) using connectionInfo.copy(shutdownCause = cause)

    case Event(Abort(cause), connectionInfo) =>
      consumer ! Consumer.Abort
      self ! Nudge
      goto(Stopping) using connectionInfo.copy(shutdownCause = cause)

    case Event(e, s) =>
      log.error("received unhandled request {} in state {}/{}", e, stateName, s)
      stay
  }

  onTermination {
    case StopEvent(_, _, connectionInfo) =>
      connectionInfo.channelActor.foreach { a =>
        context.system stop a
      }
      subscription._closedP.tryComplete(
        connectionInfo.shutdownCause.map(Failure(_)).getOrElse(Success(Unit))
      )
      stop()
  }

  initialize()

  override def preStart: Unit = {
    import ExecutionContext.Implicits.global
    val system = context.system
    // TODO - this code stinks, big time. Move to state machine
    subscription._closingP.future.foreach { timeout =>
      self ! Stop(None, timeout)
    }
    subscription.aborting.onComplete { _ =>
      self ! Abort(None)
    }
    connection ! CreateChannel(ChannelActor.props({(channel: Channel, channelActor: ActorRef) =>
      log.info(s"Channel created; ${channel}")
      self ! ChannelConnected(channel, channelActor)
    }))
  }

  // some errors close the channel and cause the error to come through async as the shutdown cause.
  private def withShutdownCatching[T](channel:Channel)(fn: => T): Either[ShutdownSignalException, T] = {
    try {
      Right(fn)
    } catch {
      case e: IOException if ! channel.isOpen && ! channel.getCloseReason.isHardError => // hardError = true indicates connection issue, false = channel issue.
        Left(channel.getCloseReason())
    }
  }

  def subscribe(connectionInfo: SubscriptionPayload) = {
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

object SubscriptionActor {
  sealed trait State
  case object Paused extends State
  case object Running extends State
  case object Stopping extends State
  case object Stopped extends State

  sealed trait Commands
  def props(subscription: Subscription, connection: ActorRef): Props =
    Props(classOf[SubscriptionActor], subscription, connection)

  case class Stop(shutdownCause: Option[ShutdownSignalException], timeout: FiniteDuration) extends Commands
  case class Abort(shutdownCause: Option[ShutdownSignalException]) extends Commands
  case object Nudge extends Commands
  case class SubscriptionPayload(channelActor: Option[ActorRef], qos: Int, consumerStopped: Boolean, shutdownCause: Option[ShutdownSignalException])
}
