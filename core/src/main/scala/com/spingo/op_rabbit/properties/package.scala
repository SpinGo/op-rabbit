package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.BasicProperties.Builder
import com.rabbitmq.client.AMQP.BasicProperties
import java.util.Date

package object properties {
  type ToHeaderValue[T, V <: HeaderValue] = T => V
  type HeaderMap = java.util.HashMap[String, Object]
  trait MessageProperty extends ((Builder, HeaderMap) => Unit)

  private [op_rabbit] trait PropertyExtractor[T] {
    def extractorName: String = getClass.getSimpleName.replace("$", "")
    def unapply(properties: BasicProperties): Option[T]
  }

  /**
    See [[Header]]
    */
  case class UnboundHeader(name: String) extends (HeaderValue => Header) with PropertyExtractor[HeaderValue] {
    override val extractorName = s"Header(${name})"
    def unapply(properties: BasicProperties) =
      for {
        h <- Option(properties.getHeaders)
        v <- Option(h.get(name))
      } yield HeaderValue.from(v)

    def apply(value: HeaderValue) = Header(name, value)
  }


  trait UnboundTypedHeader[T] extends (T => TypedHeader[T]) with PropertyExtractor[T] {
    val name: String
    protected implicit val toHeaderValue: ToHeaderValue[T, HeaderValue]
    protected val fromHeaderValue: FromHeaderValue[T]

    override final def extractorName = s"Header($name)"

    final def unapply(properties: BasicProperties): Option[T] = {
      UnboundHeader(name).unapply(properties) flatMap { hv =>
        fromHeaderValue(hv) match {
          case Right(v) => Some(v)
          case Left(ex) => None
        }
      }
    }

    final def apply(value: T) =
      TypedHeader(name, value)

    final def untyped: UnboundHeader =
      UnboundHeader(name)
  }

  protected case class UnboundTypedHeaderImpl[T](name: String)(implicit protected val fromHeaderValue: FromHeaderValue[T], protected val toHeaderValue: ToHeaderValue[T, HeaderValue]) extends UnboundTypedHeader[T]

  class TypedHeader[T] protected (val name: String, val value: T)(implicit converter: ToHeaderValue[T, HeaderValue]) extends MessageProperty {
    def apply(headers: HeaderMap): Unit =
      headers.put(name, converter(value).serializable)

    def apply(builder: Builder, headers: HeaderMap): Unit =
      this(headers)

    def untyped: Header =
      Header(name, converter(value))
  }
  object TypedHeader {
    def apply[T](name: String, value: T)(implicit converter: ToHeaderValue[T, HeaderValue]): TypedHeader[T] =
      new TypedHeader(name, value)

    def apply[T](headerName: String)(implicit conversion: FromHeaderValue[T], converter: ToHeaderValue[T, HeaderValue]): UnboundTypedHeader[T] =
      UnboundTypedHeaderImpl(headerName)

    implicit def typedHeaderToHeader[T](h: TypedHeader[T]): Header = h.untyped
  }

  /**
    Named header.

    Note, you can instantiate an UnboundHeader for use in both reading and writing the header:

    {{{
    val RetryHeader = Header("x-retry")

    Subscription {
      channel() {
        consume(queue("name")) {
          property(RetryHeader.as[Int]) { retries =>
            // ...
            ack
          }
        }
      }
    }

    rabbitControl ! QueueMessage("My body", "name", Seq(RetryHeader(5)))
    }}}

    Note, Headers are generally untyped and restricted to a limited set of primitives. Op-Rabbit uses Scala's type system to impede you from providing an invalid type.

    Application use only: RabbitMQ does not read, set, or use this parameter.
    */
  class Header protected (val name: String, val value: HeaderValue) extends MessageProperty {
    def apply(headers: HeaderMap): Unit =
      headers.put(name, value.serializable)

    def apply(builder: Builder, headers: HeaderMap): Unit =
      this(headers)
  }

  object Header {
    def apply(name: String, value: HeaderValue): Header = {
      if (value == null)
        new Header(name, HeaderValue.NullHeaderValue)
      else
        new Header(name, value)
    }
    def apply(headerName: String) = UnboundHeader(headerName)
    def unapply(header: Header): Option[(String, HeaderValue)] =
      Some((header.name, header.value))
  }

  /**
    Optional contentType message-property. Op-Rabbit's serialization layer will automatically set / read this for you.

    Application use only: RabbitMQ does not read, set, or use this parameter.
    */
  case class ContentType(contentType: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit =
      builder.contentType(contentType)
  }
  object ContentType extends PropertyExtractor[String] {
    def unapply(properties: BasicProperties) =
      Option(properties.getContentType)
  }

  /**
    Optional contentEncoding message-property. Op-Rabbit's serialization layer will automatically set / read this for you.

    Application use only: RabbitMQ does not read, set, or use this parameter.
    */
  case class ContentEncoding(contentEncoding: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit =
      builder.contentEncoding(contentEncoding)
  }
  object ContentEncoding extends PropertyExtractor[String] {
    def unapply(properties: BasicProperties) =
      Option(properties.getContentEncoding)
  }

  /**
    Property used to inform RabbitMQ of message handling; IE - should the message be persisted to disk, in case of a Broker restart?

    RabbitMQ uses an integer to represent these two states, 1 for non-persistent, 2 for persistent. To reduce confusion, Op-Rabbit maps these integers to a boolean.

    Note, RabbitMQ's default behavior is to '''NOT''' persist messages. Also, it is pointless to deliver persistent messages to a non-durable message queue. Further, non-persistent messages in a durable queue '''WILL NOT''' survive broker restart (unless replication has been configured using an [[https://www.rabbitmq.com/ha.html HA policy]]).
    */
  case class DeliveryModePersistence(persistent: Boolean = false) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.deliveryMode(if (persistent) 2 else 1)
    }
  }
  object DeliveryModePersistence extends PropertyExtractor[Boolean]{
    def unapply(properties: BasicProperties) =
      Option(properties.getDeliveryMode == 2)
    val nonPersistent = DeliveryModePersistence(false)
    val persistent = DeliveryModePersistence(true)
  }

  /**
    Property used to inform RabbitMQ of message priority. Destination queue must be configured as a priority queue. [[https://www.rabbitmq.com/priority.html Priority Queue Support]].
    */
  case class Priority(priority: Int) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit =
      builder.priority(priority)
  }
  object Priority extends PropertyExtractor[Int] {
    def unapply(properties: BasicProperties) =
      Option(properties.getPriority).map(_.toInt)
  }

  /**
    Optional correlationId message-property. Useful when doing RPC over message queue. When requesting a response, set correlationId to some unique string value (a UUID?); when responding, send this same correlationId.

    Application use only: RabbitMQ does not read, set, or use this parameter.
    */
  case class CorrelationId(id: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit =
      builder.correlationId(id)
  }
  object CorrelationId extends PropertyExtractor[String] {
    def unapply(properties: BasicProperties) =
      Option(properties.getCorrelationId)
  }

  /**
    Optional replyTo message-property. Useful when doing RPC over message queues.

    Application use only: RabbitMQ does not read, set, or use this parameter.
    */
  case class ReplyTo(replyTo: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit =
      builder.replyTo(replyTo)
  }
  object ReplyTo extends PropertyExtractor[String] {
    def unapply(properties: BasicProperties) =
      Option(properties.getReplyTo)
  }

  /**
    Optional expiration message-property.

    Application use only: RabbitMQ does not read, set, or use this parameter. If you want messages to expire, see [[https://www.rabbitmq.com/ttl.html TTL]].
    */
  case class Expiration(expiration: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.expiration(expiration)
    }
  }
  object Expiration extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getExpiration)
  }

  /**
    Optional messageId message-property.

    Application use only: RabbitMQ does not read, set, or use this parameter.
    */
  case class MessageId(messageId: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.messageId(messageId)
    }
  }

  object MessageId extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getMessageId)
  }

  /**
    Optional timestamp message-property.

    Application use only: RabbitMQ does not read, set, or use this parameter.
    */
  case class Timestamp(timestamp: Date) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.timestamp(timestamp)
    }
  }
  object Timestamp extends PropertyExtractor[Date]{
    def unapply(properties: BasicProperties) =
      Option(properties.getTimestamp)
  }

  /**
    Optional Type message-property.

    Application use only: RabbitMQ does not read, set, or use this parameter.
    */
  case class Type(`type`: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.`type`(`type`)
    }
  }
  object Type extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getType)
  }

  /**
    Optional User Id message-property. This property '''IS''' used by RabbitMQ, and should '''VERY LIKELY NOT''' be used with application User Ids. For more information, read [[http://www.rabbitmq.com/validated-user-id.html]]
    */
  case class UserId(userId: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.userId(userId)
    }
  }
  object UserId extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getUserId)
  }

  /**
    Optional application identifier message-property. Useful to help indicate which application generated a message.

    Application use only: RabbitMQ does not read, set, or use this parameter.
    */
  case class AppId(appId: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.appId(appId)
    }
  }
  object AppId extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getAppId)
  }

  case class ClusterId(clusterId: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.clusterId(clusterId)
    }
  }
  object ClusterId extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getClusterId)
  }

  implicit class PimpedBasicProperties(original: BasicProperties) {
    /**
      Returns a copy of the properties with the provided properties set.

      This is far more efficient to use than `+`. Use it when you are setting multiple properties.
      */
    def ++(others: TraversableOnce[MessageProperty]): BasicProperties =
      builderWithProperties(others, original.builder(), Option(original.getHeaders()).map(new java.util.HashMap[String, Object](_))).build

    def +(other: MessageProperty): BasicProperties =
      this ++ Seq(other)
  }

  def builderWithProperties(properties: TraversableOnce[MessageProperty], builder: Builder = new Builder(), headers: Option[HeaderMap] = None): Builder = {
    val h = headers getOrElse { new java.util.HashMap[String, Object] }
    builder.headers(h)
    properties foreach { p => p(builder, h) }
    builder
  }
}
