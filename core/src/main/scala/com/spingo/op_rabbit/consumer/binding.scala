package com.spingo.op_rabbit.consumer

import com.spingo.op_rabbit.RabbitControl
import com.spingo.op_rabbit.properties.Header
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
  @param exchangeName The name of the exchange on which we should listen for said topics. Defaults to configured exchange-name, `op-rabbit.topic-exchange-name`.
  @param durable      Specifies whether or not the message queue contents should survive a broker restart; default false.
  @param exclusive    Specifies whether or not other connections can see this connection; default false.
  @param autoDelete   Specifies whether this message queue should be deleted when the connection is closed; default false.
  @param exchangeDurable Specifies whether or not the exchange should survive a broker restart; default to `durable` parameter value.
  */
case class TopicBinding(
  queueName: String,
  topics: List[String],
  exchangeName: String = RabbitControl topicExchangeName,
  durable: Boolean = true,
  exclusive: Boolean = false,
  autoDelete: Boolean = false,
  exchangeDurable: Option[Boolean] = None) extends Binding {

  def bind(c: Channel): Unit = {
    c.exchangeDeclare(exchangeName, "topic", exchangeDurable.getOrElse(durable))
    c.queueDeclare(queueName, durable, exclusive, autoDelete, null)
    topics foreach { c.queueBind(queueName, exchangeName, _) }
  }
}

case class TopicBindingPassive(queueName: String, topics: List[String], exchangeName: String = RabbitControl topicExchangeName) extends Binding {
  def bind(c: Channel): Unit = {
    c.exchangeDeclarePassive(exchangeName)
    c.queueDeclarePassive(queueName)
    topics foreach { c.queueBind(queueName, exchangeName, _)}
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

/**
  Passively connect to a queue. If the queue does not exist already,
  then the subscription fails. (and will not retry).

  See RabbitMQ Java client docs, [[https://www.rabbitmq.com/releases/rabbitmq-java-client/v3.5.4/rabbitmq-java-client-javadoc-3.5.4/com/rabbitmq/client/Channel.html#queueDeclarePassive(jva.lang.String) Channel.queueDeclarePassive]].
  */
case class QueueBindingPassive(queueName: String) extends Binding {
  def bind(c: Channel): Unit = {
    c.queueDeclarePassive(queueName)
  }
}

/**
  Idempotently declare a headers exchange, and queue. Bind queue using
  modeled Header properties.

  It's important to note that the type matters in
  matching. Header("thing", 1) and Header("thing", "1") and seen
  differently to RabbitMQ.

  The op-rabbit Header properties class is modeled, such that the
  compiler will not allow you to specify a type that RabbitMQ does not
  support. Scala maps / seqs are appropriately converted.

  @param queueName    The name of the message queue to declare; the consumer paired with this binding will pull from this.
  @param exchangeName The name of the fanout exchange; idempotently declared.
  @param headers      List of modeled Header values used for matching.
  @param matchAll     Should all headers match in order to route the message to this queue? Default true. If false, then any if any of the match headers are matched, route the message. Pointless if only one match header defined.
  @param durable      Specifies whether or not the message queue contents should survive a broker restart; default false.
  @param exclusive    Specifies whether or not other connections can see this connection; default false.
  @param autoDelete   Specifies whether this message queue should be deleted when the connection is closed; default false.
  @param exchangeDurable Specifies whether or not the exchange should survive a broker restart; default to `durable` parameter value.
  */
case class HeadersBinding(
  queueName: String,
  exchangeName: String,
  headers: Seq[com.spingo.op_rabbit.properties.Header],
  matchAll: Boolean = true,
  durable: Boolean = true,
  exclusive: Boolean = false,
  autoDelete: Boolean = false,
  exchangeDurable: Option[Boolean] = None) extends Binding {
  def bind(c: Channel): Unit = {
    c.exchangeDeclare(exchangeName, "headers", exchangeDurable.getOrElse(durable))
    val bindingArgs = new java.util.HashMap[String, Object]
    bindingArgs.put("x-match", if (matchAll) "all" else "any") //any or all
    headers.foreach { case Header(name, value) =>
      bindingArgs.put(name, value.serializable)
    }
    c.queueDeclare(queueName, durable, exclusive, autoDelete, null)
    c.queueBind(queueName, exchangeName, "", bindingArgs);
  }
}


/**
  Idempotently declare a fanout exchange, and queue. Bind queue to exchange.

  @param queueName    The name of the message queue to declare; the consumer paired with this binding will pull from this.
  @param exchangeName The name of the fanout exchange; idempotently declared.
  @param durable      Specifies whether or not the message queue contents should survive a broker restart; default false.
  @param exclusive    Specifies whether or not other connections can see this connection; default false.
  @param autoDelete   Specifies whether this message queue should be deleted when the connection is closed; default false.
  @param exchangeDurable Specifies whether or not the exchange should survive a broker restart; default to `durable` parameter value.
  */
case class FanoutBinding(
  queueName: String,
  exchangeName: String,
  durable: Boolean = true,
  exclusive: Boolean = false,
  autoDelete: Boolean = false,
  exchangeDurable: Option[Boolean] = None) extends Binding {
  def bind(c: Channel): Unit = {
    c.exchangeDeclare(exchangeName, "fanout", exchangeDurable.getOrElse(durable))
    c.queueDeclare(queueName, durable, exclusive, autoDelete, null)
    c.queueBind(queueName, exchangeName, "", null);
  }
}
