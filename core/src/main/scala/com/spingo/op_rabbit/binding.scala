package com.spingo.op_rabbit

import com.thenewmotion.akka.rabbitmq.Channel

trait Binding {
  val queueName: String
  protected [op_rabbit] def bind(channel: Channel): Unit
}

case class TopicBinding(
  queueName: String,
  topics: List[String],
  exchangeName: String = RabbitControl topicExchangeName,
  durable: Boolean = true,
  exclusive: Boolean = false,
  autoDelete: Boolean = false,
  exchangeDurable: Boolean = true) extends Binding {

  def bind(c: Channel): Unit = {
    c.exchangeDeclare(exchangeName, "topic", exchangeDurable)
    c.queueDeclare(queueName, durable, exclusive, autoDelete, null)
    topics foreach { c.queueBind(queueName, RabbitControl.topicExchangeName, _) }
  }
}

// ConsumerTag(c.basicConsume(queueName, false, consumer.bind(c, queueName)))
case class QueueBinding(
  queueName: String,
  durable: Boolean = true,
  exclusive: Boolean = false,
  autoDelete: Boolean = false) extends Binding {

  def bind(c: Channel): Unit = {
    c.queueDeclare(queueName, durable, exclusive, autoDelete, null)
  }
}
