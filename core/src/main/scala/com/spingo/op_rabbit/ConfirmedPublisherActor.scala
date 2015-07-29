package com.spingo.op_rabbit

import akka.actor.Stash
import akka.actor.Terminated
import akka.actor.{Actor, ActorRef}
import com.rabbitmq.client.ConfirmListener
import com.rabbitmq.client.{Channel, ShutdownListener, ShutdownSignalException}
import com.thenewmotion.akka.rabbitmq.{ChannelActor, ChannelCreated, CreateChannel}
import scala.collection.mutable
import scala.concurrent.Promise

case object Nacked extends Exception(s"Message was nacked")

/**
  This actor handles confirmed message publications
  */
class ConfirmedPublisherActor(connection: ActorRef) extends Actor with Stash {
  case object ChannelDisconnected
  case class Ack(deliveryTag: Long, multiple: Boolean)
  case class Nack(deliveryTag: Long, multiple: Boolean)

  override def preStart =
    connection ! CreateChannel(ChannelActor.props({ (channel, actor) =>
      channel.confirmSelect()
      channel.addConfirmListener(new ConfirmListener {
        def handleAck(deliveryTag: Long, multiple: Boolean): Unit =
          self ! Ack(deliveryTag, multiple)
        def handleNack(deliveryTag: Long, multiple: Boolean): Unit =
          self ! Nack(deliveryTag, multiple)
      })
      channel.addShutdownListener(new ShutdownListener {
        def shutdownCompleted(cause: ShutdownSignalException): Unit = {
          self ! ChannelDisconnected
        }
      })
      self ! channel
    }), Some("confirmed-publisher-channel"))

  override def postStop =
    channelActor foreach (context stop _)

  var channelActor: Option[ActorRef] = None
  val pendingConfirmation = mutable.LinkedHashMap.empty[Long, (ConfirmedMessage, ActorRef)]
  val heldMessages = mutable.Queue.empty[(ConfirmedMessage, ActorRef)]
  val deadLetters = context.system.deadLetters

  val waitingForChannelActor: Receive = {
    case ChannelCreated(_channelActor) =>
      channelActor = Some(_channelActor)
      context.watch(_channelActor)
      context.become(waitingForChannel)
      unstashAll()
    case _ =>
      stash()
  }

  val waitingForChannel: Receive = {
    case channel: Channel =>
      context.become(connected(channel))
      unstashAll()
      // Publish any messages that were held while we were disconnected (includes published but not yet confirmed before disconnect)
      heldMessages foreach { m =>
        handleDelivery(channel, m._1, m._2)
      }
      heldMessages.clear()
    case message: ConfirmedMessage =>
      heldMessages.enqueue((message, sender))
    case ChannelDisconnected =>
      ()
    case Terminated(actorRef) if Some(actorRef) == channelActor =>
      context stop self
    case _ =>
      stash()
  }

  def connected(channel: Channel): Receive = {
    case channel: Channel =>
      context.become(connected(channel))
    case ChannelDisconnected =>
      // First, mark any messages that have not been confirmed yet but were published as held, so they will be redelivered.
      pendingConfirmation.values.foreach(heldMessages.enqueue(_))
      pendingConfirmation.clear
      context.become(waitingForChannel)
    case Terminated(actorRef) if Some(actorRef) == channelActor =>
      context stop self
    case message: ConfirmedMessage =>
      handleDelivery(channel, message, sender)
    case Ack(deliveryTag, multiple) =>
      handleAck(resolveTags(deliveryTag, multiple))(true)
    case Nack(deliveryTag, multiple) =>
      handleAck(resolveTags(deliveryTag, multiple))(false)
  }

  def handleDelivery(channel: Channel, message: ConfirmedMessage, replyTo: ActorRef): Unit = {
    val nextDeliveryTag = channel.getNextPublishSeqNo()
    message(channel)
    pendingConfirmation(nextDeliveryTag) = (message, replyTo)
  }

  def handleAck(deliveryTags: Iterable[Long])(acked: Boolean): Unit = {
    deliveryTags foreach { tag =>
      val pending = pendingConfirmation(tag)
      if (pending._2 !=  deadLetters)
        pending._2 ! acked
      pendingConfirmation.remove(tag)
    }
  }

  def pendingUpTo(deliveryTag: Long): Iterable[Long] =
    pendingConfirmation.keys.takeWhile(_ != deliveryTag)

  def resolveTags(deliveryTag: Long, multiple: Boolean): Iterable[Long] =
    if(multiple)
      pendingUpTo(deliveryTag) ++ Seq(deliveryTag)
    else
      Seq(deliveryTag)

  val receive = waitingForChannelActor
}
