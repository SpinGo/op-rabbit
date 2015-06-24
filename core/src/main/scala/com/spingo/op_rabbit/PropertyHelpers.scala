package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.BasicProperties
import com.spingo.op_rabbit.properties.{Header, builderWithProperties}
/**
  Helper functions used internally to manipulate getting and setting custom headers
  */
object PropertyHelpers {
  private val RETRY_HEADER_NAME = "RETRY"

  def getRetryCount(properties: BasicProperties) =
    Header(RETRY_HEADER_NAME).unapply(properties).map(_.asString.toInt) getOrElse (0)

  def setRetryCount(properties: BasicProperties, count: Int) = {
    import java.util.HashMap
    val p = Header(RETRY_HEADER_NAME, count)

    val map = Option(properties.getHeaders) map (new HashMap[String, Object](_)) getOrElse { new HashMap[String, Object] }

    builderWithProperties(Seq(p), properties.builder, map).build
  }
}
