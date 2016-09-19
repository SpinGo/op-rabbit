package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.BasicProperties.Builder
import com.rabbitmq.client.AMQP.BasicProperties
import java.util.Date
import scala.concurrent.duration._

package object properties {

  import com.spingo.op_rabbit.Rejection.ParseExtractRejection

  type ToHeaderValue[T, V <: HeaderValue] = T => V
  type HeaderMap = java.util.HashMap[String, Object]
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
