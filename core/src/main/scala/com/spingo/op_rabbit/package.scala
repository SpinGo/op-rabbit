package com.spingo

import scala.concurrent.{Promise,Future}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import shapeless._

package object op_rabbit {

  /**
    Represents a message delivery for usage in consumers / Handlers.
    */
  case class Delivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte])
  type Result = Either[Rejection, AckOrNack]
  type Handler = (Promise[Result], Delivery) => Unit
  type Directive1[T] = Directive[::[T, HNil]]
  type Deserialized[T] = Either[ExtractRejection, T]

  protected val futureUnit: Future[Unit] = Future.successful(Unit)

  case object Nacked extends Exception(s"Message was nacked")

  import com.rabbitmq.client.Channel
  private [op_rabbit] sealed trait AckOrNack { val deliveryTag: Long; def apply(c: Channel): Unit }
  private [op_rabbit] case class Ack(deliveryTag: Long) extends AckOrNack {
    def apply(c: Channel): Unit =
      c.basicAck(deliveryTag, false)
  }
  private [op_rabbit] case class Nack(requeue: Boolean, deliveryTag: Long) extends AckOrNack {
    def apply(c: Channel): Unit =
      c.basicReject(deliveryTag, requeue)
  }

}
