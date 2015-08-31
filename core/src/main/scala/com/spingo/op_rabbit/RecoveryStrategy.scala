package com.spingo.op_rabbit

import akka.actor.ActorSystem
import com.rabbitmq.client.AMQP.BasicProperties
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
  def apply(exception: Throwable, channel: Channel, queueName: String, actorSystem: ActorSystem): Handler
}

object RecoveryStrategy {
  object LimitedRedeliver {
    import Directives._

    type AbandonStrategy = (String, Channel, BasicProperties, Array[Byte]) => Handler
    import Queue.ModeledArgs.`x-message-ttl`

    /**
      Places messages into a queue with ".failed" appended; after ttl (default of 1 day), these messages are dropped.
      */
    def failedQueue(defaultTTL: FiniteDuration = 1 day): AbandonStrategy = { (queueName, channel, amqpProperties, body) =>
      val failureQueue = Queue.passive(
        Queue(
          s"${queueName}.failed",
          durable = true,
          arguments = Seq(`x-message-ttl`(defaultTTL))))
      failureQueue.declare(channel)
      channel.basicPublish("", failureQueue.queueName, amqpProperties, body)
      ack
    }

    val drop: AbandonStrategy = { (queueName, channel, amqpProperties, body) =>
      Directives.nack(false)
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
    import Directives._
    val `x-retry` = properties.TypedHeader[Int]("x-retry")

    private val getRetryCount = (property(`x-retry`) | provide(0))
    def apply(ex: Throwable, channel: Channel, queueName: String, actorSystem: ActorSystem): Handler = {
      import actorSystem.dispatcher


      (getRetryCount & extract(_.properties) & body(as[Array[Byte]])) { (thisRetryCount, amqpProperties, body) =>
        val exceptionHeader = PropertyHelpers.makeExceptionHeader(ex)
        if (thisRetryCount < retryCount) {
          ack {
            akka.pattern.after(redeliverDelay, actorSystem.scheduler) {
              channel.basicPublish("",
                queueName,
                amqpProperties ++ Seq(`x-retry`(thisRetryCount + 1), exceptionHeader),
                body)
              // By returning a successful future, we cause the consumer to ack the original message; this is desired since we have delivered a new copy
              // This is safe because if the redeliver fails because of a lost connection, then so will the ack.
              Future.successful(true)
            }
          }
        } else {
          onAbandon(queueName,
            channel,
            amqpProperties + exceptionHeader,
            body)
        }
      }
    }
  }

  implicit def default = limitedRedeliver()

  /**
    Nack the message. Note, when requeue = true, RabbitMQ will potentially redeliver the message forever; in most cases, this is undesirable.
    */
  def nack(requeue: Boolean = false): RecoveryStrategy = new RecoveryStrategy {
    def apply(ex: Throwable, channel: Channel, queueName: String, actorSystem: ActorSystem): Handler =
      Directives.nack(requeue)
  }

  /**
    No recovery strategy; cause the consumer to shutdown on exception.
    */
  val none: RecoveryStrategy = new RecoveryStrategy {
    def apply(ex: Throwable, channel: Channel, queueName: String, actorSystem: ActorSystem): Handler =
      Directives.ack(Future.failed(ex))
  }
}
