package com.spingo.op_rabbit

import com.rabbitmq.client.Channel
import com.rabbitmq.client.AMQP.BasicProperties
import com.spingo.op_rabbit.Binding._

trait Publisher {
  val exchangeName: String
  val routingKey: String
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit
}
/**
  Publishes messages to specified exchange, with the specified routingKey

  @param exchange The exchange to which the strategy will publish the message
  @param routingKey The routing key (or topic)
  */
private class PublisherImpl(val exchangeName: String, val routingKey: String) extends Publisher {
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit =
    c.basicPublish(exchangeName, routingKey, properties, data)
}

object Publisher {
  def queue(queueName: String): Publisher =
    new PublisherImpl("", queueName)
  def queue(queue: QueueDefinition[Concreteness]): Publisher =
    new DefiningPublisher(queue, "", queue.queueName)

  def topic(routingKey: String, exchangeName: String): Publisher =
    new PublisherImpl(exchangeName, routingKey)
  def topic(routingKey: String): Publisher =
    topic(routingKey, RabbitControl.topicExchangeName)
  def topic(routingKey: String, exchange: ExchangeDefinition[Concreteness]): Publisher =
    new DefiningPublisher(exchange, exchange.exchangeName, routingKey)

  def exchange(exchangeName: String, routingKey: String): Publisher =
    new PublisherImpl(exchangeName, routingKey)
  def exchange(exchangeName: String): Publisher =
    exchange(exchangeName, "")
  def exchange(exchange: ExchangeDefinition[Concreteness], routingKey: String): Publisher =
    new DefiningPublisher(exchange, exchange.exchangeName, routingKey)
  def exchange(exchange: ExchangeDefinition[Concreteness]): Publisher =
    new DefiningPublisher(exchange, exchange.exchangeName, "")
}

/**
  Publishes messages directly to the specified message-queue; on first message, verifies that the destination queue exists, returning an exception if not.

  This is useful if you want to prevent publishing to a non-existent queue
  */
private class DefiningPublisher(
  topologyDefinition: TopologyDefinition,
  exchangeName: String,
  routingKey: String) extends PublisherImpl(exchangeName, routingKey) {
  private var verified = false
  override def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit = {
    if (!verified) {
      RabbitHelpers.tempChannel(c.getConnection) { c =>
        topologyDefinition.declare(c)
      } match {
        case Left(ex) => throw ex
        case _ => ()
      }
      verified = true
    }
    super.apply(c,data,properties)
  }
}
