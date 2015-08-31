package com.spingo.op_rabbit

import org.scalatest.{ FunSpec, Matchers }
import com.rabbitmq.client.AMQP.BasicProperties
import com.spingo.op_rabbit.properties.PimpedBasicProperties

class PropertyHelpersSpec extends FunSpec with Matchers {
  def emptyProperties = (new BasicProperties.Builder).build
  describe("getRetryCount") {
    it("defaults to 0") {
      PropertyHelpers.getRetryCount(emptyProperties) should be (0)
    }

    it("returns the value if it's a string") {
      PropertyHelpers.getRetryCount(emptyProperties + PropertyHelpers.RetryHeader.untyped("5")) should be (5)
    }

    it("returns the value if it's an int") {
      PropertyHelpers.getRetryCount(emptyProperties + PropertyHelpers.RetryHeader(5)) should be (5)
    }
  }
}
