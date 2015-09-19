package com.spingo.op_rabbit

import com.spingo.op_rabbit.properties.Header
import com.thenewmotion.akka.rabbitmq.Channel
import com.spingo.op_rabbit.binding._

/**
  Binding which declares a message queue, and then binds various topics to it. Note that bindings are idempotent.

  @see [[Queue]], [[HeadersBinding]], [[FanoutBinding]], [[Subscription]]

  @param queue        The queue which will receive the messages matching the topics
  @param topics       A list of topics to bind to the message queue. Examples: "stock.*.nyse", "stock.#"
  @param exchange     The topic exchange over which to listen for said topics. Defaults to durable, configured topic exchange, as named by configuration `op-rabbit.topic-exchange-name`.
  */
case class TopicBinding(
  queue: QueueDefinition[Concrete],
  topics: Seq[String],
  exchange:
      ExchangeDefinition[Concrete] with
      Exchange[Exchange.Topic.type] =
    Exchange.topic(RabbitControl topicExchangeName)
) extends Binding {
  val queueName = queue.queueName
  val exchangeName = exchange.exchangeName
  def declare(c: Channel): Unit = {
    exchange.declare(c)
    queue.declare(c)
    topics foreach { c.queueBind(queueName, exchange.exchangeName, _) }
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
  @param exchangeName The name of the headers exchange; idempotently declared. Note: using the same name for a headersExchange as a topic exchange will result in errors.
  @param headers      List of modeled Header values used for matching.
  @param matchAll     Should all headers match in order to route the message to this queue? Default true. If false, then any if any of the match headers are matched, route the message. Pointless if only one match header defined.
  @param durable      Specifies whether or not the message queue contents should survive a broker restart; default false.
  @param exclusive    Specifies whether or not other connections can see this connection; default false.
  @param autoDelete   Specifies whether this message queue should be deleted when the connection is closed; default false.
  @param exchangeDurable Specifies whether or not the exchange should survive a broker restart; default to `durable` parameter value.
  */
case class HeadersBinding(
  queue: QueueDefinition[Concrete],
  exchange: ExchangeDefinition[Concrete] with Exchange[Exchange.Headers.type],
  headers: Seq[com.spingo.op_rabbit.properties.Header],
  matchAll: Boolean = true,
  exchangeDurable: Boolean = true) extends Binding {
  val queueName = queue.queueName
  val exchangeName = exchange.exchangeName
  def declare(c: Channel): Unit = {
    exchange.declare(c)
    queue.declare(c)
    val bindingArgs = new java.util.HashMap[String, Object]
    bindingArgs.put("x-match", if (matchAll) "all" else "any") //any or all
    headers.foreach { case Header(name, value) =>
      bindingArgs.put(name, value.serializable)
    }
    c.queueBind(queueName, exchange.exchangeName, "", bindingArgs);
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
  queue: QueueDefinition[Concrete],
  exchange: ExchangeDefinition[Concrete] with Exchange[Exchange.Fanout.type]) extends Binding {
  val exchangeName = exchange.exchangeName
  val queueName = queue.queueName
  def declare(c: Channel): Unit = {
    exchange.declare(c)
    queue.declare(c)
    c.queueBind(queueName, exchange.exchangeName, "", null);
  }
}
