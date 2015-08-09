package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.BasicProperties.Builder
import com.rabbitmq.client.AMQP.BasicProperties
import java.util.Date

package object properties {
  type Converter[T] = T => HeaderValue
  type HeaderMap = java.util.HashMap[String, Object]
  trait MessageProperty extends ((Builder, HeaderMap) => Unit)

  trait PropertyExtractor[T] {
    def extractorName: String = getClass.getSimpleName.replace("$", "")
    def unapply(properties: BasicProperties): Option[T]
  }

  case class UnboundHeader(name: String) extends (HeaderValue => Header) with PropertyExtractor[HeaderValue] {
    override val extractorName = s"Header(${name})"
    def unapply(properties: BasicProperties) =
      for {
        h <- Option(properties.getHeaders)
        v <- Option(h.get(name))
      } yield HeaderValue.from(v)

    def apply(value: HeaderValue) = Header(extractorName, value)
  }

  class Header protected (val name: String, val value: HeaderValue) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      headers.put(name, value.serializable)
    }
  }
  object Header {
    def apply(name: String, value: HeaderValue): Header = {
      if (value == null)
        new Header(name, NullHeaderValue)
      else
        new Header(name, value)
    }
    def apply(headerName: String) = UnboundHeader(headerName)
    def unapply(header: Header): Option[(String, HeaderValue)] =
      Some((header.name, header.value))
  }
  case class ContentType(contentType: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit =
      builder.contentType(contentType)
  }
  object ContentType extends PropertyExtractor[String] {
    def unapply(properties: BasicProperties) =
      Option(properties.getContentType)
  }

  case class ContentEncoding(contentEncoding: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit =
      builder.contentEncoding(contentEncoding)
  }
  object ContentEncoding extends PropertyExtractor[String] {
    def unapply(properties: BasicProperties) =
      Option(properties.getContentEncoding)
  }

  case class DeliveryMode(mode: Int) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.deliveryMode(mode)
    }
  }
  object DeliveryMode extends PropertyExtractor[Int]{
    def unapply(properties: BasicProperties) =
      Option(properties.getDeliveryMode)
    val nonPersistent = DeliveryMode(1)
    val persistent = DeliveryMode(2)
  }

  case class Priority(priority: Int) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit =
      builder.priority(priority)
  }
  object Priority extends PropertyExtractor[Int] {
    def unapply(properties: BasicProperties) =
      Option(properties.getPriority).map(_.toInt)
  }

  case class CorrelationId(id: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit =
      builder.correlationId(id)
  }
  object CorrelationId extends PropertyExtractor[String] {
    def unapply(properties: BasicProperties) =
      Option(properties.getCorrelationId)
  }

  //   String correlationId,
  case class ReplyTo(replyTo: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit =
      builder.replyTo(replyTo)
  }
  object ReplyTo extends PropertyExtractor[String] {
    def unapply(properties: BasicProperties) =
      Option(properties.getReplyTo)
  }

  case class Expiration(expiration: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.expiration(expiration)
    }
  }
  object Expiration extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getExpiration)
  }

  case class MessageId(messageId: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.messageId(messageId)
    }
  }
  object MessageId extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getMessageId)
  }

  case class Timestamp(timestamp: Date) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.timestamp(timestamp)
    }
  }
  object Timestamp extends PropertyExtractor[Date]{
    def unapply(properties: BasicProperties) =
      Option(properties.getTimestamp)
  }

  case class Type(`type`: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.`type`(`type`)
    }
  }
  object Type extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getType)
  }

  case class UserId(userId: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.userId(userId)
    }
  }
  object UserId extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getUserId)
  }

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

  def builderWithProperties(properties: TraversableOnce[MessageProperty], builder: Builder = new Builder(), headers: HeaderMap = new java.util.HashMap[String, Object]): Builder = {
    properties foreach { p => p(builder, headers) }
    builder.headers(headers)
    builder
  }
}
