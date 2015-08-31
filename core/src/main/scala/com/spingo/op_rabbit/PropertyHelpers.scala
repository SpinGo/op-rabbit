package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.BasicProperties
import com.spingo.op_rabbit.properties.{Header,TypedHeader}
/**
  Helper functions used internally to manipulate getting and setting custom headers
  */
private [op_rabbit] object PropertyHelpers {
  val RetryHeader = TypedHeader[Int]("x-retry")
  val ExceptionHeader = TypedHeader[String]("x-exception")

  def getRetryCount(properties: BasicProperties): Int =
    RetryHeader.extract(properties) getOrElse 0

  def makeExceptionHeader(ex: Throwable) = {
    val b = new java.io.StringWriter
    ex.printStackTrace(new java.io.PrintWriter(b))
    ExceptionHeader(b.toString)
  }
}
