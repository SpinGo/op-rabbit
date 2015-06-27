package com.spingo.op_rabbit

import scala.concurrent.{Promise,Future}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope

package object consumer {



  /**
    Represents a message delivery for usage in consumers / Handlers.
    */
  case class Delivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte])
  type Result = Either[Rejection, Unit]
  type Handler = (Promise[Result], Delivery) => Unit

  protected [op_rabbit] val futureUnit: Future[Unit] = Future.successful(Unit)

}
