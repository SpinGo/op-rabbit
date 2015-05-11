package com.spingo.op_rabbit

import org.scalatest.{ FunSpec, Matchers }
import com.rabbitmq.client.AMQP.BasicProperties

class PropertyHelpersSpec extends FunSpec with Matchers {
  def emptyProperties = (new BasicProperties.Builder).build
  describe("getRetryCount") {
    it("defaults to 0") {
      PropertyHelpers.getRetryCount(emptyProperties) should be (0)
    }
  }

  describe("setRetryCount") {
    it("sets the property to a header that getRetryCount can read") {
      val properties = PropertyHelpers.setRetryCount(emptyProperties, 5)
      PropertyHelpers.getRetryCount(properties) should be (5)
    }
  }
}
