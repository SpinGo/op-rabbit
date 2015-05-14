package com.spingo.op_rabbit

import akka.actor.{ActorSystem, Props}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.thenewmotion.akka.rabbitmq.Channel

/**
 This trait describes the interface used by the Subscription to instantiate the Consumer Actor.
 */
trait Consumer {

  /**
    The name of the actor; must be unique per consumer instance, since the consumer name informs the subscription actor name.
    */
  val name: String

  /**
    Used by the subscription; returns ActorProps necessary for instantiating the consumer actor implementation.

   Consumer Actor Contract:

   - must support `Unsubscribe`, `Shutdown`, `Abort`, and `Subscribe(channel)` signals (@see [[Consumer$ Consumer companion object]])
   - should expect Subscribe to be sent multiple times in a row in case of connection failure
   - must reply to sender with "true" when Unsubscribe sent
   - must stop itself after Shutdown message is received (and all pending work is complete)
   */

  def props(queueName: String): Props
}

object Consumer {
  sealed trait ConsumerCommand

  /**
    Configure a subscription for a new channel
    */
  case class Subscribe(channel: Channel) extends ConsumerCommand

  /**
    Tell the consumer to stop processing new messages; don't close the
    connection, don't stop, continue to acknowledge any messages in
    process.
    */
  case object Unsubscribe extends ConsumerCommand

  /**
   Graceful shutdown; immediately stop receiving new messages, and
   wait for any pending messages to be acknowledged, and then stops the
   actor
   */
  case object Shutdown extends ConsumerCommand

  /**
   Like shutdown, but does not wait for pending messages to be
   acknowledged before stopping the actor
   */
  case object Abort extends ConsumerCommand

  /**
    For internal use to the actor itself; the RabbitMQ DefaultConsumer
    should send one of these to the Consumer actor.
    */
  case class Delivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]) extends ConsumerCommand
}
