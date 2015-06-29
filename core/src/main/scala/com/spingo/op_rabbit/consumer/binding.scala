package com.spingo.op_rabbit.consumer

import com.spingo.op_rabbit.RabbitControl
import com.thenewmotion.akka.rabbitmq.Channel

/**
  Implementors of this trait describe how a channel is defined, and
  how bindings are associated.
  */
trait Binding {
  /**
    The name of the message queue this binding declares.
    */
  val queueName: String
  protected [op_rabbit] def bind(channel: Channel): Unit
}

/**
  Binding which declares a message queue, and then binds various topics to it. Note that bindings are idempotent.

  @see [[QueueBinding]], [[Subscription]]

  @param queueName    The name of the message queue to declare; the consumer paired with this binding will pull from this.
  @param topics       A list of topics to bind to the message queue. Examples: "stock.*.nyse", "stock.#"
  @param exchangeName The name of the exchange on which we should listen for said topics. Defaults to configured exchange-name, `rabbitmq.topic-exchange-name`.
  @param durable      Specifies whether or not the message queue contents should survive a broker restart; default false.
  @param exclusive    Specifies whether or not other connections can see this connection; default false.
  @param autoDelete   Specifies whether this message queue should be deleted when the connection is closed; default false.
  @param exchangeDurable Specifies whether or not the exchange should survive a broker restart; default true.
  */
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

/**
  Binding which declare a message queue, without any exchange or topic bindings.

  @see [[TopicBinding]], [[Subscription]]

  @param queueName    The name of the message queue to declare; the consumer paired with this binding will pull from this.
  @param durable      Specifies whether or not the message queue contents should survive a broker restart; default false.
  @param exclusive    Specifies whether or not other connections can see this connection; default false.
  @param autoDelete   Specifies whether this message queue should be deleted when the connection is closed; default false.
  */
case class QueueBinding(
  queueName: String,
  durable: Boolean = true,
  exclusive: Boolean = false,
  autoDelete: Boolean = false) extends Binding {

  def bind(c: Channel): Unit = {
    c.queueDeclare(queueName, durable, exclusive, autoDelete, null)
  }
}
