package com.spingo.op_rabbit

sealed trait ReceiveResult { val deliveryTag: Long }

object ReceiveResult {
  case class Ack(deliveryTag: Long) extends ReceiveResult
  object Ack extends (Long => Ack) {
    def apply(delivery: Delivery): Ack =
      Ack(delivery.envelope.getDeliveryTag)
  }

  case class Nack(deliveryTag: Long, requeue: Boolean) extends ReceiveResult
  object Nack extends ((Long, Boolean) => Nack) {
    def apply(delivery: Delivery, requeue: Boolean): Nack =
      Nack(delivery.envelope.getDeliveryTag, requeue)
  }

  case class Fail(delivery: Delivery, message: Option[String], exception: Throwable) extends ReceiveResult {
    val deliveryTag = delivery.envelope.getDeliveryTag
  }
}
