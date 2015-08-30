package com.spingo.op_rabbit

import com.rabbitmq.client.ShutdownSignalException
import com.rabbitmq.client.{Connection,Channel}
import java.io.IOException

object RabbitHelpers {
  // some errors close the channel and cause the error to come through async as the shutdown cause.
  def withChannelShutdownCatching[T](channel:Channel)(fn: => T): Either[ShutdownSignalException, T] =
    try {
      Right(fn)
    } catch {
      case e: IOException if ! channel.isOpen && ! channel.getCloseReason.isHardError => // hardError = true indicates connection issue, false = channel issue.
        Left(channel.getCloseReason())
    }

  def tempChannel[T](connection: Connection)(fn: Channel => T): Either[ShutdownSignalException, T] = {
    val tempChannel = connection.createChannel
    try {
      withChannelShutdownCatching(tempChannel) { fn(tempChannel) }
    } finally if (tempChannel.isOpen()) tempChannel.close()
  }
}
