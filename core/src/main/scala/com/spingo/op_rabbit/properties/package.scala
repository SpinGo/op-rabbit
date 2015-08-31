package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.BasicProperties.Builder
import com.rabbitmq.client.AMQP.BasicProperties
import java.util.Date
import scala.concurrent.duration._

package object properties {

  import com.spingo.op_rabbit.ParseExtractRejection

  type ToHeaderValue[T, V <: HeaderValue] = T => V
  type HeaderMap = java.util.HashMap[String, Object]
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
    See [[Header]]
    */
  case class UnboundHeader(name: String) extends (HeaderValue => Header) with PropertyExtractor[HeaderValue] with HashMapExtractor[HeaderValue] {
    override val extractorName = s"Header(${name})"
    def extract(properties: BasicProperties) =
      Option(properties.getHeaders).flatMap(extract)

    def extract(h: java.util.Map[String, Object]) =
      Option(h.get(name)) map (HeaderValue.from(_))

    def apply(value: HeaderValue) = Header(name, value)
  }


  trait UnboundTypedHeader[T] extends (T => TypedHeader[T]) with PropertyExtractor[T] with HashMapExtractor[T] {
    val name: String
    protected implicit val toHeaderValue: ToHeaderValue[T, HeaderValue]
    protected val fromHeaderValue: FromHeaderValue[T]

    override final def extractorName = s"Header($name)"

    final def extract(properties: BasicProperties): Option[T] =
      Option(properties.getHeaders).flatMap(extract)

    final def extract(m: java.util.Map[String, Object]): Option[T] =
      UnboundHeader(name).extract(m) flatMap { hv =>
        fromHeaderValue(hv) match {
          case Left(ex) => throw new ParseExtractRejection("Header ${name} exists, but value could not be converted to ${fromHeaderValue.manifest}", ex)
          case Right(v) => Some(v)
        }
      }

    final def apply(value: T) =
      TypedHeader(name, value)

    final def untyped: UnboundHeader =
      UnboundHeader(name)
  }

  private [op_rabbit] case class UnboundTypedHeaderLongToFiniteDuration(name: String) extends UnboundTypedHeader[FiniteDuration] {
    protected val toHeaderValue = { d: FiniteDuration => HeaderValue(d.toMillis) }
    protected val fromHeaderValue = implicitly[FromHeaderValue[Long]].map(_ millis)
  }

  private [op_rabbit] case class UnboundTypedHeaderImpl[T](name: String)(implicit protected val fromHeaderValue: FromHeaderValue[T], protected val toHeaderValue: ToHeaderValue[T, HeaderValue]) extends UnboundTypedHeader[T]

  /**
    TypedHeader describes a RabbitMQ Message Header or
    Queue/Exchange/Binding argument; its type is known, and implicit
    proof is in scope that the type can be safely converted to a known
    RabbitMQ type.

    If you use custom headers, using TypedHeader is preferred over Header. For example, if you use the header `host-ip`, and this header contains a string, use the following:

    {{{val `host-ip` = TypedHeader[Int]("host-ip")}}}

    You can use this header to both set your `host-ip` message header, and lift it out in your consumer using the [[Directives.property property]] directive.
    */
  class TypedHeader[T] protected (val name: String, val value: T)(implicit converter: ToHeaderValue[T, HeaderValue]) extends MessageProperty {
    def insert(headers: HeaderMap): Unit =
      headers.put(name, converter(value).serializable)

    def insert(builder: Builder, headers: HeaderMap): Unit =
      insert(headers)

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
    def insert(headers: HeaderMap): Unit =
      headers.put(name, value.serializable)

    def insert(builder: Builder, headers: HeaderMap): Unit =
      insert(headers)
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
    Property used to inform RabbitMQ of message handling; IE - should the message be persisted to disk, in case of a Broker restart?

    RabbitMQ uses an integer to represent these two states, 1 for non-persistent, 2 for persistent. To reduce confusion, Op-Rabbit maps these integers to a boolean.

    Note, RabbitMQ's default behavior is to '''NOT''' persist messages. Also, it is pointless to deliver persistent messages to a non-durable message queue. Further, non-persistent messages in a durable queue '''WILL NOT''' survive broker restart (unless replication has been configured using an [[https://www.rabbitmq.com/ha.html HA policy]]).
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
    Property used to inform RabbitMQ of message priority. Destination queue must be configured as a priority queue. [[https://www.rabbitmq.com/priority.html Priority Queue Support]].
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
    Optional correlationId message-property. Useful when doing RPC over message queue. When requesting a response, set correlationId to some unique string value (a UUID?); when responding, send this same correlationId.

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

    Application use only: RabbitMQ does not read, set, or use this parameter. If you want messages to expire, see [[https://www.rabbitmq.com/ttl.html TTL]].
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
    Optional User Id message-property. This property '''IS''' used by RabbitMQ, and should '''VERY LIKELY NOT''' be used with application User Ids. For more information, read [[http://www.rabbitmq.com/validated-user-id.html]]
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

  def toJavaMap(headers: Seq[Header], existingMap: HeaderMap = null): HeaderMap = {
    if (headers.isEmpty)
      null
    else {
      val m = if (existingMap == null) new java.util.HashMap[String, Object] else existingMap
      headers.foreach(header => header insert m)
      m
    }
  }

  def builderWithProperties(properties: TraversableOnce[MessageProperty], builder: Builder = new Builder(), headers: Option[HeaderMap] = None): Builder = {
    val m = headers getOrElse { new java.util.HashMap[String, Object] }
    builder.headers(m)
    properties foreach { p => p.insert(builder, m) }
    builder
  }
}
