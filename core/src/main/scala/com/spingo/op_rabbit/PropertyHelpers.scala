package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.BasicProperties

import scala.collection.JavaConversions._
import com.rabbitmq.client.LongString
import com.rabbitmq.client.impl.LongStringHelper

object PropertyHelpers {
  private val RETRY_HEADER_NAME = "RETRY"

  def getRetryCount(properties: BasicProperties) =
    Option(properties.getHeaders).
      flatMap { h => Option(h.get(RETRY_HEADER_NAME).asInstanceOf[LongString]) }.
      map { ls => new String(ls.getBytes).toInt }.
      getOrElse(0)

  def setRetryCount(properties: BasicProperties, count: Int) = {
    val retryEntry = (RETRY_HEADER_NAME -> LongStringHelper.asLongString(count.toString))
    val updatedHeaders: Map[String, Object] = Option(properties.getHeaders) map { _.toMap + retryEntry } getOrElse { Map(retryEntry) }

    properties.
      builder.
      headers(updatedHeaders).
      build
  }

}
