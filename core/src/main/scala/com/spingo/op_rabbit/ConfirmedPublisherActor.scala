package com.spingo.op_rabbit

import akka.actor.{Actor, ActorRef, Stash, Status, Terminated}
import com.rabbitmq.client.{Channel, ConfirmListener, ShutdownListener, ShutdownSignalException}
import com.spingo.op_rabbit.RabbitHelpers.withChannelShutdownCatching
import com.thenewmotion.akka.rabbitmq.{ChannelActor, ChannelCreated, CreateChannel}
import scala.collection.mutable

/**
  This actor handles confirmed message publications
  */
private [op_rabbit] class ConfirmedPublisherActor(connection: ActorRef) extends Actor with Stash {
  case object ChannelDisconnected
  private sealed trait InternalAckOrNack
  private case class Ack(deliveryTag: Long, multiple: Boolean) extends InternalAckOrNack
  private case class Nack(deliveryTag: Long, multiple: Boolean) extends InternalAckOrNack

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
  val pendingConfirmation = mutable.LinkedHashMap.empty[Long, (Message, ActorRef)]
  val heldMessages = mutable.Queue.empty[(Message, ActorRef)]

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
    case message: Message =>
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
    case message: Message =>
      handleDelivery(channel, message, sender)
    case Ack(deliveryTag, multiple) =>
      handleAck(resolveTags(deliveryTag, multiple))(true)
    case Nack(deliveryTag, multiple) =>
      handleAck(resolveTags(deliveryTag, multiple))(false)
  }

  def handleDelivery(channel: Channel, message: Message, replyTo: ActorRef): Unit = {
    val nextDeliveryTag = channel.getNextPublishSeqNo()
    try {
      withChannelShutdownCatching(channel) {
        message(channel)
      } match {
        case Left(ex) =>
          replyTo ! Message.Fail(message.id, ex)
        case _ =>
          pendingConfirmation(nextDeliveryTag) = (message, replyTo)
      }
    } catch {
      case ex: Throwable =>
        replyTo ! Message.Fail(message.id, ex)
    }
  }

  private val deadLetters = context.system.deadLetters

  def handleAck(deliveryTags: Iterable[Long])(acked: Boolean): Unit = {
    deliveryTags foreach { tag =>
      val (pendingMessage, pendingSender) = pendingConfirmation(tag)
      if (pendingSender != deadLetters)
        pendingSender ! (if (acked) Message.Ack(pendingMessage.id) else Message.Nack(pendingMessage.id))
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
