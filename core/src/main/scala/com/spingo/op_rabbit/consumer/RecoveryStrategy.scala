package com.spingo.op_rabbit.consumer

import akka.actor.ActorSystem
import com.rabbitmq.client.Channel
import com.spingo.op_rabbit.PropertyHelpers
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  Instructs rabbitmq what to do when an unhandled exceptions occur; by default, simply nack the message.

  By contract, RecoveryStrategy returns a Future[Boolean], which is interpretted as follows:

  - Success(true) acks the message, consumer continues with it's business
  - Success(false) nacks the message, consumer continues with it's business
  - Failure(ex) nacks the message, and causes the consumer to stop.
  */
abstract class RecoveryStrategy {
  def apply(exception: Throwable, channel: Channel, queueName: String, delivery: Delivery): Future[Boolean]
}

object RecoveryStrategy {
  /* TODO - implement retry according to this pattern: http://yuserinterface.com/dev/2013/01/08/how-to-schedule-delay-messages-with-rabbitmq-using-a-dead-letter-exchange/
     - create two direct exchanges: work and retry
     - Publish to rety with additional header set/incremented: x-redelivers
       - { "x-dead-letter-exchange", WORK_EXCHANGE },
       - { "x-message-ttl", RETRY_DELAY }
           (setting since nothing consumes the direct exchange, it will go back to retry queue)
   */
  def limitedRedeliver(redeliverDelay: FiniteDuration = 10 seconds, retryCount: Int = 3)(implicit actorSystem: ActorSystem) = new RecoveryStrategy {

    def apply(ex: Throwable, channel: Channel, queueName: String, delivery: Delivery): Future[Boolean] = {
      import actorSystem.dispatcher
      val thisRetryCount = PropertyHelpers.getRetryCount(delivery.properties)
      if (thisRetryCount < retryCount)
        akka.pattern.after(redeliverDelay, actorSystem.scheduler) {
          val withRetryCountIncremented = PropertyHelpers.setRetryCount(delivery.properties, thisRetryCount + 1)
          channel.basicPublish("", queueName, withRetryCountIncremented, delivery.body)
          // By returning a successful future, we cause the consumer to ack the original message; this is desired since we have delivered a new copy
          // This is safe because if the redeliver fails because of a lost connection, then so will the ack.
          Future.successful(true)
        }
        else
          // Give up; ack original message, don't redeliver
          Future.successful(true)
    }
  }

  /**
    Default strategy is to let the exception through; nack the message.
    */
  implicit val default: RecoveryStrategy = new RecoveryStrategy {
    def apply(ex: Throwable, channel: Channel, queueName: String, delivery: Delivery): Future[Boolean] = {
      Future.successful(false)
    }
  }

  val none: RecoveryStrategy = new RecoveryStrategy {
    def apply(ex: Throwable, channel: Channel, queueName: String, delivery: Delivery): Future[Boolean] =
      Future.failed(ex)
  }
}
