package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.BasicProperties.Builder
import com.rabbitmq.client.LongString

package object props {
  type HeaderMap = java.util.Map[String, Object]
  trait MessageProperty extends ((Builder, HeaderMap) => Unit)

  // These two properties are handled by the marshaller
  //   String contentType,
  //   String contentEncoding,

  // If your header value is larger than Java allows... you've got issues, man.
  case class Header(name: String, value: LongString) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      headers.put(name, value)
    }
  }
  object Header {
    def apply(name: String, value: String): Header =
      Header(name, com.rabbitmq.client.impl.LongStringHelper.asLongString(value))
  }
  case class DeliveryMode(mode: Int) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.deliveryMode(mode)
    }
  }

  object DeliveryMode {
    val nonPersistent = DeliveryMode(1)
    val persistent = DeliveryMode(2)
  }

  case class Priority(priority: Int) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.priority(priority)
    }
  }

  case class CorrelationId(id: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.correlationId(id)
    }
  }

  //   String correlationId,
  //   ,
  case class ReplyTo(replyTo: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.replyTo(replyTo)
    }
  }

  case class Expiration(expiration: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.expiration(expiration)
    }
  }

  case class MessageId(messageId: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.messageId(messageId)
    }
  }

  case class Timestamp(timestamp: java.util.Date) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.timestamp(timestamp)
    }
  }

  case class Type(`type`: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.`type`(`type`)
    }
  }

  case class UserId(userId: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.userId(userId)
    }
  }

  case class AppId(appId: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.appId(appId)
    }
  }

  case class ClusterId(clusterId: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.clusterId(clusterId)
    }
  }

  def applyTo(properties: TraversableOnce[MessageProperty], builder: Builder = new Builder()): Builder = {
    val headers = new java.util.HashMap[String, Object]
    // apply default properties
    DeliveryMode.persistent(builder, headers)
    // apply provided properties
    properties foreach { p => p(builder, headers) }
    builder.headers(headers)
    builder
  }
}
