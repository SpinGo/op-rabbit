package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.BasicProperties
import com.spingo.op_rabbit.properties.Header
/**
  Helper functions used internally to manipulate getting and setting custom headers
  */
object PropertyHelpers {
  val RetryHeader = Header("x-retry")
  val ExceptionHeader = Header("x-exception")

  def getRetryCount(properties: BasicProperties): Int =
    properties match {
      case RetryHeader(v) =>
        v.asOpt[Int].get
      case _ =>
        0
    }

  def makeExceptionHeader(ex: Throwable) = {
    val b = new java.io.StringWriter
    ex.printStackTrace(new java.io.PrintWriter(b))
    ExceptionHeader(b.toString)
  }
}
