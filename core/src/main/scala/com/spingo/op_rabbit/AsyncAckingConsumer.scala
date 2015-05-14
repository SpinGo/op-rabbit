package com.spingo.op_rabbit

import akka.actor.{ActorSystem, Props}
import scala.concurrent.duration._
import scala.concurrent.Future

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
