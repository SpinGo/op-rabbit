package com.spingo.op_rabbit.subscription

import akka.actor.ActorSystem
import com.rabbitmq.client.Channel
import com.spingo.op_rabbit.Consumer.Delivery
import com.spingo.op_rabbit.PropertyHelpers
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  Instructs rabbitmq what to do when an unhandled exceptions occur; by default, simply nack the message.
  */
abstract class RecoveryStrategy {
  def apply(exception: Throwable, channel: Channel, queueName: String, delivery: Delivery): Future[Boolean]
}

object RecoveryStrategy {
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
  implicit val defaultStrategy: RecoveryStrategy = new RecoveryStrategy {
    def apply(ex: Throwable, channel: Channel, queueName: String, delivery: Delivery): Future[Boolean] = {
      Future.successful(false)
    }
  }
}
