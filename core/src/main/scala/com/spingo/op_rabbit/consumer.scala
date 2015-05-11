package com.spingo.op_rabbit

import akka.actor.{ActorSystem, Props}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.thenewmotion.akka.rabbitmq.Channel
import scala.concurrent.Future
import scala.concurrent.duration._

trait Consumer {
  val name: String
  def props(queueName: String): Props
}

object Consumer {
  sealed trait ConsumerCommand

  case class Subscribe(channel: Channel) extends ConsumerCommand

  case object Unsubscribe extends ConsumerCommand

  /*
   Graceful shutdown; immediately stop receiving new messages, and
   wait for any pending messages to be acknowledged, and then stops the
   actor
   */
  case object Shutdown extends ConsumerCommand

  /*
   Like shutdown, but does not wait for pending messages to be
   acknowledged before stopping the actor
   */
  case object Abort extends ConsumerCommand
  case class Delivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]) extends ConsumerCommand


  /*
   Contact:

   - must support Unsubscribe, Shutdown, Abort, and Subscribe
   - should expect Subscribe to be sent multiple times in a row in case of connection failure
   - must reply to sender with "true" when Unsubscribe sent
   - must stop itself after Shutdown message is received (and all pending work is complete)
   */


}

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
