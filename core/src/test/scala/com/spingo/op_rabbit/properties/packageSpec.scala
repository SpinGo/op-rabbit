package com.spingo.op_rabbit.properties

import org.scalatest.{FunSpec, Matchers}
import com.spingo.op_rabbit.properties._
import com.rabbitmq.client.impl.LongStringHelper
import com.rabbitmq.client.AMQP.BasicProperties.Builder
import com.rabbitmq.client.AMQP.BasicProperties

class PropertiesSpec extends FunSpec with Matchers {
  describe("Setting properties") {
    it("merges multiple HeaderValues") {
      val properties = builderWithProperties(Seq(Header("header1", "very-value1"), Header("header2", "very-value2"))).build
      properties.getHeaders.get("header1") should be ("very-value1")
      properties.getHeaders.get("header2") should be ("very-value2")
    }
  }

  describe("matchers") {
    val properties = builderWithProperties(
      Seq(
        DeliveryMode.persistent,
        ReplyTo("reply-destination"),
        Header("header1", "very-value1"),
        Header("header2", "very-value2"))).build
    it("lifts out property values") {
      val DeliveryMode(mode) = properties
      mode should be (2)

      val ReplyTo(where) = properties
      where should be ("reply-destination")

      Header("header1").unapply(properties) should be (Some(HeaderValue("very-value1")))
      Header("header2").unapply(properties) should be (Some(HeaderValue("very-value2")))
    }
  }
}
