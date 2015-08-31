package com.spingo.op_rabbit

import com.spingo.op_rabbit.properties.Header
import com.thenewmotion.akka.rabbitmq.Channel
import scala.concurrent.duration._

/**
  Implementors of this trait describe how a channel is defined, and
  how bindings are associated.

  TODO - rename to QueueDefinitionLike
  */
trait QueueDefinitionLike {
  /**
    The name of the message queue this binding declares.
    */
  val queueName: String
  def declare(channel: Channel): Unit
}

trait ExchangeDefinition[+T <: ExchangeDefinition.Value] {
  val name: String
  def declare(channel: Channel): Unit
}

private class ExchangeDefinitionImpl[+T <: ExchangeDefinition.Value](val name: String, kind: T, durable: Boolean, autoDelete: Boolean, arguments: Seq[Header]) extends ExchangeDefinition[T] {
  def declare(c: Channel): Unit =
    c.exchangeDeclare(name, kind.toString, durable, autoDelete, arguments.foldLeft(new java.util.HashMap[String, Object]) { (m, header) => header(m); m })
}

/**
  Passively declare an exchange. If the queue does not exist already,
  then either try the fallback binding, or fail.

  See RabbitMQ Java client docs, [[https://www.rabbitmq.com/releases/rabbitmq-java-client/v3.5.4/rabbitmq-java-client-javadoc-3.5.4/com/rabbitmq/client/Channel.html#exchangeDeclarePassive(java.lang.String) Channel.exchangeDeclarePassive]].
  */
private class ExchangeDefinitionPassive[T <: ExchangeDefinition.Value](val name: String, ifNotDefined: Option[ExchangeDefinition[T]] = None) extends ExchangeDefinition[T] {
  def declare(channel: Channel): Unit = {
    RabbitHelpers.tempChannel(channel.getConnection) { t =>
      t.exchangeDeclarePassive(name)
    }.left.foreach { (ex =>
      ifNotDefined.map(_.declare(channel)) getOrElse { throw ex })
    }
  }
}

object ExchangeDefinition extends Enumeration {
  val Topic = Value("topic")
  val Headers = Value("headers")
  val Fanout = Value("fanout")
  val Direct = Value("direct")

  def topic(name: String, durable: Boolean = true, autoDelete: Boolean = false, arguments: Seq[Header] = Seq()): ExchangeDefinition[ExchangeDefinition.Topic.type] = new ExchangeDefinitionImpl(name: String, ExchangeDefinition.Topic, durable, autoDelete, arguments)
  def headers(name: String, durable: Boolean = true, autoDelete: Boolean = false, arguments: Seq[Header] = Seq()): ExchangeDefinition[ExchangeDefinition.Headers.type] = new ExchangeDefinitionImpl(name: String, ExchangeDefinition.Headers, durable, autoDelete, arguments)
  def fanout(name: String, durable: Boolean = true, autoDelete: Boolean = false, arguments: Seq[Header] = Seq()): ExchangeDefinition[ExchangeDefinition.Fanout.type] = new ExchangeDefinitionImpl(name: String, ExchangeDefinition.Fanout, durable, autoDelete, arguments)
  def direct(name: String, durable: Boolean = true, autoDelete: Boolean = false, arguments: Seq[Header] = Seq()): ExchangeDefinition[ExchangeDefinition.Direct.type] = new ExchangeDefinitionImpl(name: String, ExchangeDefinition.Direct, durable, autoDelete, arguments)

  def passive(exchangeName: String): ExchangeDefinition[Nothing] = new ExchangeDefinitionPassive(exchangeName, None)
  def passive[T <: ExchangeDefinition.Value](binding: ExchangeDefinition[T]): ExchangeDefinition[T] = new ExchangeDefinitionPassive(binding.name, Some(binding))
}

/**
  Binding which declares a message queue, and then binds various topics to it. Note that bindings are idempotent.

  @see [[QueueBinding]], [[Subscription]]

  @param queue        The queue which will receive the messages matching the topics
  @param topics       A list of topics to bind to the message queue. Examples: "stock.*.nyse", "stock.#"
  @param exchange     The topic exchange over which to listen for said topics. Defaults to durable, configured topic exchange, as named by configuration `op-rabbit.topic-exchange-name`.
  */
case class TopicBinding(
  queue: QueueDefinition,
  topics: List[String],
  exchange: ExchangeDefinition[ExchangeDefinition.Topic.type] = ExchangeDefinition.topic(RabbitControl topicExchangeName)
) extends QueueDefinitionLike {
  val queueName = queue.queueName
  def declare(c: Channel): Unit = {
    exchange.declare(c)
    queue.declare(c)
    topics foreach { c.queueBind(queueName, exchange.name, _) }
  }
}

/**
  Passively connect to a queue. If the queue does not exist already,
  then either try the fallback binding, or fail.

  This is useful when binding to a queue defined by another process, and with pre-existing properties set, such as `x-message-ttl`.

  See RabbitMQ Java client docs, [[https://www.rabbitmq.com/releases/rabbitmq-java-client/v3.5.4/rabbitmq-java-client-javadoc-3.5.4/com/rabbitmq/client/Channel.html#queueDeclarePassive(jva.lang.String) Channel.queueDeclarePassive]].
  */
private class QueueBindingPassive(val queueName: String, ifNotDefined: Option[QueueDefinitionLike] = None) extends QueueDefinitionLike {
  def declare(channel: Channel): Unit = {
    RabbitHelpers.tempChannel(channel.getConnection) { t =>
      t.queueDeclarePassive(queueName)
    }.left.foreach { (ex =>
      ifNotDefined.map(_.declare(channel)) getOrElse { throw ex })
    }
  }
}

object ModeledQueueArgs {
  /**
    This message header causes RabbitMQ to drop any messages in a queue longer after the specified time.
    */
  val `x-message-ttl` = new properties.UnboundTypedHeader[FiniteDuration] {
    import properties._
    val name = "x-message-ttl"
    protected val toHeaderValue = { d: FiniteDuration => HeaderValue(d.toMillis) }
    protected val fromHeaderValue = implicitly[FromHeaderValue[Long]].map(_ millis)
  }
}
/**
  Binding which declare a message queue, without any exchange or topic bindings.

  @see [[TopicBinding]], [[Subscription]]

  @param queueName    The name of the message queue to declare; the consumer paired with this binding will pull from this.
  @param durable      Specifies whether or not the message queue contents should survive a broker restart; default false.
  @param exclusive    Specifies whether or not other connections can see this connection; default false.
  @param autoDelete   Specifies whether this message queue should be deleted when the connection is closed; default false.
  @param arguments    Special arguments for this queue, such as x-message-ttl
  */
case class QueueDefinition(
  queueName: String,
  durable: Boolean = true,
  exclusive: Boolean = false,
  autoDelete: Boolean = false,
  arguments: Seq[Header] = Seq()
) extends QueueDefinitionLike {

  def declare(c: Channel): Unit = {
    c.queueDeclare(queueName, durable, exclusive, autoDelete,
      if (arguments.isEmpty)
        null
      else
        arguments.foldLeft(new java.util.HashMap[String, Object]) { (m, header) => header(m); m }
    )
  }
}

object QueueDefinition {
  def passive(queueName: String): QueueDefinitionLike = new QueueBindingPassive(queueName, None)
  def passive(binding: QueueDefinitionLike): QueueDefinitionLike = new QueueBindingPassive(binding.queueName, Some(binding))
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
  queue: QueueDefinition,
  exchange: ExchangeDefinition[ExchangeDefinition.Headers.type],
  headers: Seq[com.spingo.op_rabbit.properties.Header],
  matchAll: Boolean = true,
  exchangeDurable: Boolean = true) extends QueueDefinitionLike {
  val queueName = queue.queueName
  def declare(c: Channel): Unit = {
    exchange.declare(c)
    queue.declare(c)
    val bindingArgs = new java.util.HashMap[String, Object]
    bindingArgs.put("x-match", if (matchAll) "all" else "any") //any or all
    headers.foreach { case Header(name, value) =>
      bindingArgs.put(name, value.serializable)
    }
    c.queueBind(queueName, exchange.name, "", bindingArgs);
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
  queue: QueueDefinition,
  exchange: ExchangeDefinition[ExchangeDefinition.Fanout.type]) extends QueueDefinitionLike {
  val queueName = queue.queueName
  def declare(c: Channel): Unit = {
    exchange.declare(c)
    queue.declare(c)
    c.queueBind(queueName, exchange.name, "", null);
  }
}
