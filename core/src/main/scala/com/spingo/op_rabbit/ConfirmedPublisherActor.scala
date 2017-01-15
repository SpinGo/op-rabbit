package com.spingo.op_rabbit

import akka.actor.{Actor, ActorLogging, ActorRef, Stash, Status, Terminated}
import com.rabbitmq.client.{Channel, ConfirmListener, ShutdownListener, ShutdownSignalException}
import com.newmotion.akka.rabbitmq.{ChannelActor, ChannelCreated, CreateChannel}
import scala.collection.mutable

/**
  This actor handles confirmed message publications
  */
private [op_rabbit] class ConfirmedPublisherActor(connection: ActorRef) extends Actor with Stash with ActorLogging {
  case class ChannelDisconnected(cause: ShutdownSignalException)
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
          self ! ChannelDisconnected(cause)
        }
      })
      self ! channel
    }), Some("confirmed-publisher-channel"))

  override def postStop =
    channelActor foreach (context stop _)

  private var channelActor: Option[ActorRef] = None
  private val pendingConfirmation = mutable.LinkedHashMap.empty[Long, (Message, ActorRef)]
  private val heldMessages = mutable.Queue.empty[(Message, ActorRef)]

  private var headRetries = 0
  private var headMessageId: Long = 0

  private val waitingForChannelActor: Receive = {
    case ChannelCreated(_channelActor) =>
      channelActor = Some(_channelActor)
      context.watch(_channelActor)
      context.become(waitingForChannel)
      unstashAll()
    case _ =>
      stash()
  }

  private val waitingForChannel: Receive = {
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
    case ChannelDisconnected(ex) =>
      ()
    case Terminated(actorRef) if Some(actorRef) == channelActor =>
      context stop self
    case _ =>
      stash()
  }

  private def connected(channel: Channel): Receive = {
    case channel: Channel =>
      context.become(connected(channel))
    case ChannelDisconnected(ex) =>
      if (!ex.isInitiatedByApplication) {
        log.error(ex, "Publisher channel was disconnected unexpectedly")
        dropHeadAfterMaxRetry(ex)
      }

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

  private def handleDelivery(channel: Channel, message: Message, replyTo: ActorRef): Unit = {
    val nextDeliveryTag = channel.getNextPublishSeqNo()
    try {
      message(channel)
      pendingConfirmation(nextDeliveryTag) = (message, replyTo)
    } catch {
      case ex: Throwable =>
        replyTo ! Message.Fail(message.id, ex)
    }
  }

  private val deadLetters = context.system.deadLetters

  private def handleAck(deliveryTags: Iterable[Long])(acked: Boolean): Unit = {
    deliveryTags foreach { tag =>
      val (pendingMessage, pendingSender) = pendingConfirmation(tag)
      if (pendingSender != deadLetters)
        pendingSender ! (if (acked) Message.Ack(pendingMessage.id) else Message.Nack(pendingMessage.id))
      pendingConfirmation.remove(tag)
    }
  }

  private def pendingUpTo(deliveryTag: Long): Iterable[Long] =
    pendingConfirmation.keys.takeWhile(_ != deliveryTag)

  private def resolveTags(deliveryTag: Long, multiple: Boolean): Iterable[Long] =
    if(multiple)
      pendingUpTo(deliveryTag) ++ Seq(deliveryTag)
    else
      Seq(deliveryTag)

  /** For some operations, such as publishing to a wrong exchange, it will cause
    * RabbitMQ to close the channel after we've published it. This makes it
    * difficult to tell exactly which message is the cause of the channel
    * close. If it happens 3 times while the same message is at the head of
    * pendingConfirmations, we assume that the head message is the cause of the
    * error, send the Message.Fail response, and drop it so we can continue
    * publishing messages */
  private def dropHeadAfterMaxRetry(ex: Throwable): Unit = {
    if (pendingConfirmation.isEmpty)
      return
    val (msg, replyTo) = pendingConfirmation.values.head
    if (headMessageId != msg.id) {
      headMessageId = msg.id
      headRetries = 1
    } else {
      headRetries += 1
    }

    if (headRetries >= 3) {
      pendingConfirmation.remove(pendingConfirmation.keys.head)
      replyTo ! Message.Fail(msg.id, ex)
    }
  }

  val receive = waitingForChannelActor
}
