package com.spingo.op_rabbit.properties

import com.rabbitmq.client.AMQP.BasicProperties.Builder
import com.rabbitmq.client.AMQP.BasicProperties
import java.util.Date

trait MessageProperty {
  def insert(builder: Builder, headers: HeaderMap): Unit
}

private [op_rabbit] trait PropertyExtractor[T] {
  def extractorName: String = getClass.getSimpleName.replace("$", "")
  def extract(properties: BasicProperties): Option[T]
}

private [op_rabbit] trait HashMapExtractor[T] {
  def extract(properties: java.util.Map[String, Object]): Option[T]
}


/**
  Optional contentType message-property. Op-Rabbit's serialization layer will automatically set / read this for you.

  Application use only: RabbitMQ does not read, set, or use this parameter.
  */
case class ContentType(contentType: String) extends MessageProperty {
  def insert(builder: Builder, headers: HeaderMap): Unit =
    builder.contentType(contentType)
}
object ContentType extends PropertyExtractor[String] {
  def extract(properties: BasicProperties) =
    Option(properties.getContentType)
}

/**
  Optional contentEncoding message-property. Op-Rabbit's serialization layer will automatically set / read this for you.

  Application use only: RabbitMQ does not read, set, or use this parameter.
  */
case class ContentEncoding(contentEncoding: String) extends MessageProperty {
  def insert(builder: Builder, headers: HeaderMap): Unit =
    builder.contentEncoding(contentEncoding)
}
object ContentEncoding extends PropertyExtractor[String] {
  def extract(properties: BasicProperties) =
    Option(properties.getContentEncoding)
}

/**
  Property used to inform RabbitMQ of message handling; IE - should the message be persisted to disk, in case of a
  Broker restart?

  RabbitMQ uses an integer to represent these two states, 1 for non-persistent, 2 for persistent. To reduce confusion,
  Op-Rabbit maps these integers to a boolean.

  Note, RabbitMQ's default behavior is to '''NOT''' persist messages. Also, it is pointless to deliver persistent
  messages to a non-durable message queue. Further, non-persistent messages in a durable queue '''WILL NOT''' survive
  broker restart (unless replication has been configured using an [[https://www.rabbitmq.com/ha.html HA policy]]).
  */
case class DeliveryModePersistence(persistent: Boolean = false) extends MessageProperty {
  def insert(builder: Builder, headers: HeaderMap): Unit = {
    builder.deliveryMode(if (persistent) 2 else 1)
  }
}
object DeliveryModePersistence extends PropertyExtractor[Boolean]{
  def extract(properties: BasicProperties) =
    Option(properties.getDeliveryMode == 2)
  val nonPersistent = DeliveryModePersistence(false)
  val persistent = DeliveryModePersistence(true)
}

/**
  Property used to inform RabbitMQ of message priority. Destination queue must be configured as a priority
  queue. [[https://www.rabbitmq.com/priority.html Priority Queue Support]].
  */
case class Priority(priority: Int) extends MessageProperty {
  def insert(builder: Builder, headers: HeaderMap): Unit =
    builder.priority(priority)
}
object Priority extends PropertyExtractor[Int] {
  def extract(properties: BasicProperties) =
    Option(properties.getPriority).map(_.toInt)
}

/**
  Optional correlationId message-property. Useful when doing RPC over message queue. When requesting a response, set
  correlationId to some unique string value (a UUID?); when responding, send this same correlationId.

  Application use only: RabbitMQ does not read, set, or use this parameter.
  */
case class CorrelationId(id: String) extends MessageProperty {
  def insert(builder: Builder, headers: HeaderMap): Unit =
    builder.correlationId(id)
}
object CorrelationId extends PropertyExtractor[String] {
  def extract(properties: BasicProperties) =
    Option(properties.getCorrelationId)
}

/**
  Optional replyTo message-property. Useful when doing RPC over message queues.

  Application use only: RabbitMQ does not read, set, or use this parameter.
  */
case class ReplyTo(replyTo: String) extends MessageProperty {
  def insert(builder: Builder, headers: HeaderMap): Unit =
    builder.replyTo(replyTo)
}
object ReplyTo extends PropertyExtractor[String] {
  def extract(properties: BasicProperties) =
    Option(properties.getReplyTo)
}

/**
  Optional expiration message-property.

  Application use only: RabbitMQ does not read, set, or use this parameter. If you want messages to expire, see
  [[https://www.rabbitmq.com/ttl.html TTL]].
  */
case class Expiration(expiration: String) extends MessageProperty {
  def insert(builder: Builder, headers: HeaderMap): Unit = {
    builder.expiration(expiration)
  }
}
object Expiration extends PropertyExtractor[String]{
  def extract(properties: BasicProperties) =
    Option(properties.getExpiration)
}

/**
  Optional messageId message-property.

  Application use only: RabbitMQ does not read, set, or use this parameter.
  */
case class MessageId(messageId: String) extends MessageProperty {
  def insert(builder: Builder, headers: HeaderMap): Unit = {
    builder.messageId(messageId)
  }
}

object MessageId extends PropertyExtractor[String]{
  def extract(properties: BasicProperties) =
    Option(properties.getMessageId)
}

/**
  Optional timestamp message-property.

  Application use only: RabbitMQ does not read, set, or use this parameter.
  */
case class Timestamp(timestamp: Date) extends MessageProperty {
  def insert(builder: Builder, headers: HeaderMap): Unit = {
    builder.timestamp(timestamp)
  }
}
object Timestamp extends PropertyExtractor[Date]{
  def extract(properties: BasicProperties) =
    Option(properties.getTimestamp)
}

/**
  Optional Type message-property.

  Application use only: RabbitMQ does not read, set, or use this parameter.
  */
case class Type(`type`: String) extends MessageProperty {
  def insert(builder: Builder, headers: HeaderMap): Unit = {
    builder.`type`(`type`)
  }
}
object Type extends PropertyExtractor[String]{
  def extract(properties: BasicProperties) =
    Option(properties.getType)
}

/**
  Optional User Id message-property. This property '''IS''' used by RabbitMQ, and should '''VERY LIKELY NOT''' be used
  with application User Ids. For more information, read [[http://www.rabbitmq.com/validated-user-id.html]]
  */
case class UserId(userId: String) extends MessageProperty {
  def insert(builder: Builder, headers: HeaderMap): Unit = {
    builder.userId(userId)
  }
}
object UserId extends PropertyExtractor[String]{
  def extract(properties: BasicProperties) =
    Option(properties.getUserId)
}

/**
  Optional application identifier message-property. Useful to help indicate which application generated a message.

  Application use only: RabbitMQ does not read, set, or use this parameter.
  */
case class AppId(appId: String) extends MessageProperty {
  def insert(builder: Builder, headers: HeaderMap): Unit = {
    builder.appId(appId)
  }
}
object AppId extends PropertyExtractor[String]{
  def extract(properties: BasicProperties) =
    Option(properties.getAppId)
}

case class ClusterId(clusterId: String) extends MessageProperty {
  def insert(builder: Builder, headers: HeaderMap): Unit = {
    builder.clusterId(clusterId)
  }
}
object ClusterId extends PropertyExtractor[String]{
  def extract(properties: BasicProperties) =
    Option(properties.getClusterId)
}
