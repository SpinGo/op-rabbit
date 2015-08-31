package com.spingo.op_rabbit

import akka.actor.ActorSystem
import com.rabbitmq.client.Channel
import scala.concurrent.Future
import scala.concurrent.duration._
import com.spingo.op_rabbit.properties.Header
import com.spingo.op_rabbit.properties.PimpedBasicProperties

/**
  Instructs rabbitmq what to do when an unhandled exceptions occur; by default, simply nack the message.

  By contract, RecoveryStrategy returns a Future[Boolean], which is interpreted as follows:

  - Success(true) acks the message, consumer continues with it's business
  - Success(false) nacks the message, consumer continues with it's business
  - Failure(ex) nacks the message, and causes the consumer to stop.
  */
abstract class RecoveryStrategy {
  def apply(exception: Throwable, channel: Channel, queueName: String, delivery: Delivery, actorSystem: ActorSystem): Future[Boolean]
}

object RecoveryStrategy {
  object LimitedRedeliver {
    type AbandonStrategy = (String, Channel, Delivery) => Future[Unit]
    import ModeledQueueArgs.`x-message-ttl`

    /**
      Places messages into a queue with ".failed" appended; after ttl (default of 1 day), these messages are dropped.
      */
    def failedQueue(defaultTTL: FiniteDuration = 1 day): AbandonStrategy = { (queueName, channel, delivery) =>
      val failureQueue = QueueDefinition.passive(
        QueueDefinition(
          s"${queueName}.failed",
          durable = true,
          arguments = Seq(`x-message-ttl`(defaultTTL))))
      failureQueue.declare(channel)
      channel.basicPublish("", failureQueue.queueName, delivery.properties, delivery.body)
      Future.successful(())
    }

    val drop: AbandonStrategy = { (queueName, channel, delivery) =>
      Future.successful(())
    }
  }

  /* TODO - implement retry according to this pattern: http://yuserinterface.com/dev/2013/01/08/how-to-schedule-delay-messages-with-rabbitmq-using-a-dead-letter-exchange/
     - create two direct exchanges: work and retry
     - Publish to rety with additional header set/incremented: x-redelivers
       - { "x-dead-letter-exchange", WORK_EXCHANGE },
       - { "x-message-ttl", RETRY_DELAY }
           (setting since nothing consumes the direct exchange, it will go back to retry queue)
   */
  def limitedRedeliver(redeliverDelay: FiniteDuration = 10 seconds, retryCount: Int = 3, onAbandon: LimitedRedeliver.AbandonStrategy = LimitedRedeliver.drop) = new RecoveryStrategy {

    def apply(ex: Throwable, channel: Channel, queueName: String, delivery: Delivery, actorSystem: ActorSystem): Future[Boolean] = {
      import actorSystem.dispatcher
      val thisRetryCount = PropertyHelpers.getRetryCount(delivery.properties)
      val exceptionHeader = PropertyHelpers.makeExceptionHeader(ex)
      if (thisRetryCount < retryCount)
        // NOTE !!! errors are swallowed here!!
        akka.pattern.after(redeliverDelay, actorSystem.scheduler) {
          channel.basicPublish("",
            queueName,
            delivery.properties ++ Seq(PropertyHelpers.RetryHeader(thisRetryCount + 1), exceptionHeader),
            delivery.body)
          // By returning a successful future, we cause the consumer to ack the original message; this is desired since we have delivered a new copy
          // This is safe because if the redeliver fails because of a lost connection, then so will the ack.
          Future.successful(true)
        } else {

          onAbandon(queueName,
            channel,
            delivery.copy(properties = delivery.properties + exceptionHeader))
          // Give up; ack original message, don't redeliver
          Future.successful(true)
        }
    }
  }

  implicit def default = limitedRedeliver()

  /**
    Nack the message. Note, this will cause RabbitMQ to redeliver the message forever; in most cases, this is undesirable.
    */
  val nack: RecoveryStrategy = new RecoveryStrategy {
    def apply(ex: Throwable, channel: Channel, queueName: String, delivery: Delivery, actorSystem: ActorSystem): Future[Boolean] =
      Future.successful(false)
  }

  val none: RecoveryStrategy = new RecoveryStrategy {
    def apply(ex: Throwable, channel: Channel, queueName: String, delivery: Delivery, actorSystem: ActorSystem): Future[Boolean] =
      Future.failed(ex)
  }
}
