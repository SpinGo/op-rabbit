package com.spingo.op_rabbit

import com.rabbitmq.client.Channel
import com.rabbitmq.client.ShutdownSignalException
import java.io.IOException
import scala.annotation.tailrec


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
    @tailrec def unapply(e: Throwable): Option[com.rabbitmq.client.ShutdownSignalException] = e match {
      case e: java.io.IOException =>
        unapply(e.getCause)
      case e: com.rabbitmq.client.ShutdownSignalException =>
        Some(e)
      case _ =>
        None
    }
  }

  object AlreadyClosedException {
    @tailrec def unapply(e: Throwable): Option[com.rabbitmq.client.AlreadyClosedException] = e match {
      case e: java.io.IOException =>
        unapply(e.getCause)
      case e: com.rabbitmq.client.AlreadyClosedException =>
        Some(e)
      case _ =>
        None
    }
  }

  object ConnectionGoneException {
    def unapply[T <: Throwable](e: T): Option[T] =
      e match {
        case ShuttingDownException(_)
           | AlreadyClosedException(_)
            => Some(e)
        case _ => None
      }
  }

  object NonFatalRabbitException {
    def unapply[T <: Throwable](e: T): Option[T] =
      e match {
        case UnknownConsumerTagException(_)
           | ShuttingDownException(_)
           | AlreadyClosedException(_)
            => Some(e)
        case _ => None
      }
  }
}
