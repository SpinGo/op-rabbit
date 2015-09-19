package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope

/**
  Represents a message delivery for usage in consumers / Handlers.
  */
case class Delivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte])
