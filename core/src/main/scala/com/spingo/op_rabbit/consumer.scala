package com.spingo.op_rabbit

import akka.actor.{ActorSystem, Props}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.thenewmotion.akka.rabbitmq.Channel
import scala.concurrent.Future
import scala.concurrent.duration._

/*
 This trait describes the interface used by the Subscription to instantiate the 
 */
trait Consumer {
  val name: String

  /**
   Consumer Actor Contract:

   - must support `Unsubscribe`, `Shutdown`, `Abort`, and `Subscribe(channel)` signals (@see [[Consumer$ Consumer companion object]])
   - should expect Subscribe to be sent multiple times in a row in case of connection failure
   - must reply to sender with "true" when Unsubscribe sent
   - must stop itself after Shutdown message is received (and all pending work is complete)
   */

  def props(queueName: String): Props
}

object Consumer {
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
  case object Shutdown extends ConsumerCommand

  /**
   Like shutdown, but does not wait for pending messages to be
   acknowledged before stopping the actor
   */
  case object Abort extends ConsumerCommand

  /**
    For internal use to the actor itself; the RabbitMQ DefaultConsumer
    should send one of these to the Consumer actor.
    */
  case class Delivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]) extends ConsumerCommand
}


/**
  AsyncAckingConsumer allows easy specification of a concurrent
  RabbitMQ consumer using Futures, such that the messages are only
  removed from the message-queue once the Future completes.

  Parameters:

  @param name The name of this consumer; it informs the actor name and is used to for error reporting; it must be unique.
  @param redeliverDelay When a future fails, indicates how long the consumer should wait before resubmitting the message for retry
  @param retryCount After the first attempt, retry this many times before dropping the message altogether
  @param qos This parameter specifies the number of unacknowledged messages the consumer will process at a time; for example, if you'd like to process 3 jobs concurrently at any given time, set `qos` to 3.
  @param handle This function defines the work. Note, that the function itself is called in the actor thread itself. You should avoid blocking that thread and do your work inside of a Future (Future which, by contract, should be returned by this function).
  */
case class AsyncAckingConsumer[T](
  name: String,
  redeliverDelay: FiniteDuration = 10 seconds,
  retryCount: Int = 3,
  qos: Int = 1)(
  handle: T => Future[Unit])(
  implicit
    unmarshaller: RabbitUnmarshaller[T],
    actorSystem: ActorSystem,
    rabbitErrorLogging: RabbitErrorLogging
) extends Consumer {
  import Consumer._

  def props(queueName: String) = Props {
    new impl.AsyncAckingRabbitConsumer(
      name             = name,
      queueName        = queueName,
      recoveryStrategy = impl.AsyncAckingRabbitConsumer.withRetry(queueName, redeliverDelay, retryCount),
      onChannel        = { (channel) => channel.basicQos(qos) },
      handle           = handle)
  }
}
