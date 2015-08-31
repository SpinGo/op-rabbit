package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.BasicProperties
import com.spingo.op_rabbit.properties.{Header,TypedHeader}
/**
  Helper functions used internally to manipulate getting and setting custom headers
  */
private [op_rabbit] object PropertyHelpers {
  val `x-exception` = TypedHeader[String]("x-exception")

  def makeExceptionHeader(ex: Throwable) = {
    val b = new java.io.StringWriter
    ex.printStackTrace(new java.io.PrintWriter(b))
    `x-exception`(b.toString)
  }
}
