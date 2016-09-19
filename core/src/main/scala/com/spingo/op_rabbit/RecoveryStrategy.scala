package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Channel
import scala.concurrent.Future
import scala.concurrent.duration._
import com.spingo.op_rabbit.properties.PimpedBasicProperties

/**
  Instructs rabbitmq what to do when an unhandled exceptions occur; by default, simply nack the message.

  By contract, RecoveryStrategy returns a Future[Boolean], which is interpreted as follows:

  - Success(true) acks the message, consumer continues with it's business
  - Success(false) nacks the message, consumer continues with it's business
  - Failure(ex) nacks the message, and causes the consumer to stop.
  */
abstract class RecoveryStrategy {
  def apply(exception: Throwable, channel: Channel, queueName: String): Handler
}

object RecoveryStrategy {
  object LimitedRedeliver {
    import Directives._

    type AbandonStrategy = (String, Channel, BasicProperties, Array[Byte], Throwable) => Handler
    import Queue.ModeledArgs._

    /**
      The message header used to track retry attempts.
      */
    val `x-retry` = properties.TypedHeader[Int]("x-retry")

    /** Places messages into a queue with "op-rabbit.abandoned" prepended; after ttl (default of 1 day), these messages
      * are dropped.
      */
    def abandonedQueue(
      defaultTTL: FiniteDuration = 1.day,
      abandonTo: (String) => String = { q => s"op-rabbit.abandoned.$q" }): AbandonStrategy = {
      (queueName, channel, amqpProperties, body, exception) =>
      val failureQueue = Queue.passive(
        Queue(
          abandonTo(queueName),
          durable = true,
          arguments = Seq(
            `x-message-ttl`(defaultTTL),
            `x-expires`(defaultTTL * 2)
          )))
      failureQueue.declare(channel)
      channel.basicPublish("", failureQueue.queueName, amqpProperties, body)
      ack
    }

    val drop: AbandonStrategy = { (queueName, channel, amqpProperties, body, exception) =>
      Directives.nack(false)
    }
  }

  def limitedRedeliver(
    redeliverDelay: FiniteDuration = 10.seconds,
    retryCount: Int = 3,
    onAbandon: LimitedRedeliver.AbandonStrategy = LimitedRedeliver.drop,
    retryVia : (String) => String = { q => s"op-rabbit.retry.$q" }) = new RecoveryStrategy {
    import Directives._
    import Queue.ModeledArgs._
    import LimitedRedeliver.`x-retry`

    def genRetryQueue(queueName: String) =
      Queue.passive(
        Queue(
          retryVia(queueName),
          durable = true,
          arguments = Seq(
            `x-expires`(redeliverDelay * 3),
            `x-message-ttl`(redeliverDelay),
            `x-dead-letter-exchange`(""), // default exchange
            `x-dead-letter-routing-key`(queueName))))

    private val getRetryCount = (property(`x-retry`) | provide(0))
    def apply(ex: Throwable, channel: Channel, queueName: String): Handler = {
      // import actorSystem.dispatcher

      (getRetryCount & extract(_.properties) & body(as[Array[Byte]])) { (thisRetryCount, amqpProperties, body) =>
        val exceptionHeader = PropertyHelpers.makeExceptionHeader(ex)
        if (thisRetryCount < retryCount) {
          val retryQueue = genRetryQueue(queueName)
          retryQueue.declare(channel)

          channel.basicPublish("",
            retryQueue.queueName,
            amqpProperties ++ Seq(`x-retry`(thisRetryCount + 1), exceptionHeader),
            body)
              // By returning a successful future, we cause the consumer to ack the original message; this is desired since we have delivered a new copy
              // This is safe because if the redeliver fails because of a lost connection, then so will the ack.
          ack
        } else {
          onAbandon(queueName,
            channel,
            amqpProperties + exceptionHeader,
            body,
            ex)
        }
      }
    }
  }

  implicit def default = limitedRedeliver()

  /**
    Nack the message. Note, when requeue = true, RabbitMQ will potentially redeliver the message forever; in most cases, this is undesirable.
    */
  def nack(requeue: Boolean = false): RecoveryStrategy = new RecoveryStrategy {
    def apply(ex: Throwable, channel: Channel, queueName: String): Handler =
      Directives.nack(requeue)
  }

  /**
    No recovery strategy; cause the consumer to shutdown on exception.
    */
  val none: RecoveryStrategy = new RecoveryStrategy {
    def apply(ex: Throwable, channel: Channel, queueName: String): Handler =
      Directives.ack(Future.failed(ex))
  }
}
