package com.spingo.op_rabbit

import com.spingo.op_rabbit.properties.Header
import com.rabbitmq.client.Channel

trait Binding extends Binding.QueueDefinition[Binding.Abstract]
    with Binding.ExchangeDefinition[Binding.Abstract]
object Binding {

  // TODO - rename to AnyConcreteness
  sealed trait Concreteness
  sealed trait Concrete extends Concreteness {}
  sealed trait Abstract extends Concreteness {}

  sealed trait TopologyDefinition {
    def declare(channel: Channel): Unit
  }

  trait QueueDefinition[+T <: Concreteness] extends TopologyDefinition {
    /**
      The name of the message queue this binding declares.
      */
    val queueName: String
  }

  trait ExchangeDefinition[+T <: Concreteness] extends TopologyDefinition {
    val exchangeName: String
  }

  def direct(
    queue: QueueDefinition[Concrete],
    exchange:
        ExchangeDefinition[Concrete] with
        Exchange[Exchange.Direct.type],
    routingKeys: List[String] = Nil
  ): Binding = new Binding {
    val queueName = queue.queueName
    val exchangeName = exchange.exchangeName

    def declare(c: Channel): Unit = {
      exchange.declare(c)
      queue.declare(c)
      if (routingKeys.isEmpty)
        c.queueBind(queueName, exchangeName, queueName, null)
      else
        routingKeys.foreach { routingKey =>
          c.queueBind(queueName, exchangeName, routingKey, null)
        }
    }
  }

  /**
    Binding which declares a message queue, and then binds various topics to it. Note that bindings are idempotent.

    @see [[Queue]], [[HeadersBinding]], [[FanoutBinding]], [[Subscription]]

    @param queue        The queue which will receive the messages matching the topics
    @param topics       A list of topics to bind to the message queue. Examples: "stock.*.nyse", "stock.#"
    @param exchange     The topic exchange over which to listen for said topics. Defaults to durable, configured topic exchange, as named by configuration `op-rabbit.topic-exchange-name`.
    */
  def topic(
    queue: QueueDefinition[Concrete],
    topics: Seq[String],
    exchange:
        ExchangeDefinition[Concrete] with
        Exchange[Exchange.Topic.type] =
      Exchange.topic(RabbitControl.topicExchangeName)
  ): Binding = new Binding {
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

    @param queue      The queue into which the matching messages should be delivered.
    @param exchange   The headers exchange
    @param headers    List of modeled Header values used for matching.
    @param matchAll   Should all headers match in order to route the message to this queue? Default true. If false, then any if any of the match headers are matched, route the message. Pointless if only one match header defined.
    */
  def headers(
    queue: QueueDefinition[Concrete],
    exchange: ExchangeDefinition[Concrete] with Exchange[Exchange.Headers.type],
    headers: Seq[com.spingo.op_rabbit.properties.Header],
    matchAll: Boolean = true): Binding = new Binding {
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

    @param queue      The queue into which the fanout messages should be delivered.
    @param exchange   The fanout exchange
    */
  def fanout(
    queue: QueueDefinition[Concrete],
    exchange: ExchangeDefinition[Concrete] with Exchange[Exchange.Fanout.type]): Binding = new Binding {
    val exchangeName = exchange.exchangeName
    val queueName = queue.queueName
    def declare(c: Channel): Unit = {
      exchange.declare(c)
      queue.declare(c)
      c.queueBind(queueName, exchange.exchangeName, "", null);
    }
  }

}
