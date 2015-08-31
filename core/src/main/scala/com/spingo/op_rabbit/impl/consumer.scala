package com.spingo.op_rabbit.impl

import akka.actor.{ActorSystem, Props}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.thenewmotion.akka.rabbitmq.Channel

private [op_rabbit] object Consumer {
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
  case class Shutdown(cause: Option[Throwable]) extends ConsumerCommand

  /**
   Like shutdown, but does not wait for pending messages to be
   acknowledged before stopping the actor
   */
  case class Abort(cause: Option[Throwable]) extends ConsumerCommand
}
