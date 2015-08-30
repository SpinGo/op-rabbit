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
  type Result = Either[Rejection, Unit]
  type Handler = (Promise[Result], Delivery) => Unit
  type Directive1[T] = Directive[::[T, HNil]]
  type Deserialized[T] = Either[ExtractRejection, T]

  protected val futureUnit: Future[Unit] = Future.successful(Unit)

  case object Nacked extends Exception(s"Message was nacked")
}
