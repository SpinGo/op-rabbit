package com.spingo.op_rabbit

import com.rabbitmq.client.Channel
import com.rabbitmq.client.ShutdownSignalException
import java.io.IOException


object RabbitExceptionMatchers {
  /**
   RabbitMQ java JAR isn't particularly good at defining some
   exceptions; we've got to resort to some unfortunate brute-force
   methods to match specific exceptions.
   */
  object UnknownConsumerTagException {
    def unapply(e: java.io.IOException): Option[java.io.IOException] = {
      if (e.getMessage() == "Unknown consumerTag")
        Some(e)
      else
        None
    }
  }

  object ShuttingDownException {
    def unapply(e: java.io.IOException): Option[com.rabbitmq.client.ShutdownSignalException] = e.getCause match {
      case e: com.rabbitmq.client.ShutdownSignalException =>
        Some(e)
      case _ =>
        None
    }
  }

  object AlreadyClosedException {
    def unapply(e: com.rabbitmq.client.AlreadyClosedException): Option[com.rabbitmq.client.AlreadyClosedException] =
      Some(e)
  }

  object NonFatalRabbitException {
    def unapply[T <: Throwable](e: T): Option[T] =
      e match {
        case (_: ShutdownSignalException)
           | UnknownConsumerTagException(_)
           | ShuttingDownException(_)
           | AlreadyClosedException(_)
            => Some(e)
        case _ => None
      }
  }
}
