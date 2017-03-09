package com.spingo.op_rabbit.impl

import com.newmotion.akka.rabbitmq.Channel

private [op_rabbit] object Consumer {
  sealed trait ConsumerCommand

  /**
    Configure a subscription for a new channel
    */
  case class Subscribe(channel: Channel, initialQos: Int) extends ConsumerCommand

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

}
