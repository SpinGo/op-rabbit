package com.spingo.op_rabbit

import com.spingo.op_rabbit.properties.Header
import com.thenewmotion.akka.rabbitmq.Channel

/**
  Implementors of this trait describe how a channel is defined, and
  how bindings are associated.

  TODO - rename to QueueDefinitionLike
  */
trait Binding {
  /**
    The name of the message queue this binding declares.
    */
  val queueName: String
  protected [op_rabbit] def bind(channel: Channel): Unit
}

trait ExchangeBinding[+T <: ExchangeBinding.Value] {
  val exchangeName: String
  protected [op_rabbit] def bind(channel: Channel): Unit
}

object ExchangeBinding extends Enumeration {
  val Topic = Value("topic")
  val Headers = Value("headers")
  val Fanout = Value("fanout")
  def apply(name: String, kind: ExchangeBinding.Value, durable: Boolean = true) = new ExchangeBinding[kind.type] {
    val exchangeName = name
    def bind(c: Channel): Unit =
      c.exchangeDeclare(exchangeName, kind.toString, durable)
  }

  def topic(exchangeName: String, durable: Boolean = true) = apply(exchangeName, Topic, durable)
  def headers(exchangeName: String, durable: Boolean = true) = apply(exchangeName, Headers, durable)
  def fanout(exchangeName: String, durable: Boolean = true) = apply(exchangeName, Fanout, durable)

  def passive(exchangeName: String): ExchangeBinding[Nothing] = new ExchangeBindingPassive(exchangeName, None)
  def passive[T <: ExchangeBinding.Value](binding: ExchangeBinding[T]): ExchangeBinding[T] = new ExchangeBindingPassive(binding.exchangeName, Some(binding))
}

/**
  Binding which declares a message queue, and then binds various topics to it. Note that bindings are idempotent.

  @see [[QueueBinding]], [[Subscription]]

  @param queue        The queue to which we will  of the message queue to declare; the consumer paired with this binding will pull from this.
  @param topics       A list of topics to bind to the message queue. Examples: "stock.*.nyse", "stock.#"
  @param exchangeName The name of the exchange on which we should listen for said topics. Defaults to configured exchange-name, `op-rabbit.topic-exchange-name`.
  @param durable      Specifies whether or not the message queue contents should survive a broker restart; default false.
  @param exclusive    Specifies whether or not other connections can see this connection; default false.
  @param autoDelete   Specifies whether this message queue should be deleted when the connection is closed; default false.
  @param exchangeDurable Specifies whether or not the exchange should survive a broker restart; default to `durable` parameter value.
  */
case class TopicBinding(
  queue: QueueBinding,
  topics: List[String],
  exchange: ExchangeBinding[ExchangeBinding.Topic.type] = ExchangeBinding.topic(RabbitControl topicExchangeName)
) extends Binding {
  val queueName = queue.queueName
  def bind(c: Channel): Unit = {
    exchange.bind(c)
    queue.bind(c)
    topics foreach { c.queueBind(queueName, exchange.exchangeName, _) }
  }
}

/**
  Passively connect to a queue. If the queue does not exist already,
  then either try the fallback binding, or fail.

  This is useful when binding to a queue defined by another process, and with pre-existing properties set, such as `x-message-ttl`.

  See RabbitMQ Java client docs, [[https://www.rabbitmq.com/releases/rabbitmq-java-client/v3.5.4/rabbitmq-java-client-javadoc-3.5.4/com/rabbitmq/client/Channel.html#queueDeclarePassive(jva.lang.String) Channel.queueDeclarePassive]].
  */
private class QueueBindingPassive(val queueName: String, ifNotDefined: Option[Binding] = None) extends Binding {
  def bind(channel: Channel): Unit = {
    RabbitHelpers.tempChannel(channel.getConnection) { t =>
      t.queueDeclarePassive(queueName)
    }.left.foreach { ex =>
      ifNotDefined.map(_.bind(channel)) getOrElse { throw ex }
    }
  }
}

/**
  Passively declare an exchange. If the queue does not exist already,
  then either try the fallback binding, or fail.

  See RabbitMQ Java client docs, [[https://www.rabbitmq.com/releases/rabbitmq-java-client/v3.5.4/rabbitmq-java-client-javadoc-3.5.4/com/rabbitmq/client/Channel.html#exchangeDeclarePassive(java.lang.String) Channel.exchangeDeclarePassive]].
  */
private class ExchangeBindingPassive[T <: ExchangeBinding.Value](val exchangeName: String, ifNotDefined: Option[ExchangeBinding[T]] = None) extends ExchangeBinding[T] {
  def bind(channel: Channel): Unit = {
    RabbitHelpers.tempChannel(channel.getConnection) { t =>
      t.exchangeDeclarePassive(exchangeName)
    }.left.foreach { ex =>
      ifNotDefined.map(_.bind(channel)) getOrElse { throw ex }
    }
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
  autoDelete: Boolean = false,
  arguments: Seq[Header] = Seq()
) extends Binding {

  def bind(c: Channel): Unit = {
    c.queueDeclare(queueName, durable, exclusive, autoDelete,
      if (arguments.isEmpty)
        null
      else
        arguments.foldLeft(new java.util.HashMap[String, Object]) { (m, header) => header(m); m }
    )
  }
}

object QueueBinding {
  def passive(queueName: String): Binding = new QueueBindingPassive(queueName, None)
  def passive(binding: Binding): Binding = new QueueBindingPassive(binding.queueName, Some(binding))
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
  queue: QueueBinding,
  exchange: ExchangeBinding[ExchangeBinding.Headers.type],
  headers: Seq[com.spingo.op_rabbit.properties.Header],
  matchAll: Boolean = true,
  exchangeDurable: Boolean = true) extends Binding {
  val queueName = queue.queueName
  def bind(c: Channel): Unit = {
    exchange.bind(c)
    queue.bind(c)
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
