package com.spingo.op_rabbit.properties

import org.scalatest.{FunSpec, Matchers}
import com.spingo.op_rabbit.properties._
import com.rabbitmq.client.impl.LongStringHelper
import com.rabbitmq.client.AMQP.BasicProperties.Builder
import com.rabbitmq.client.AMQP.BasicProperties

class PropertiesSpec extends FunSpec with Matchers {
  describe("HeaderValue") {
    it("tries its best to pull a string via asString") {
      val string = "I can haz le string"
      HeaderValue(string.getBytes).asString should be (string)
      HeaderValue(LongStringHelper.asLongString(string)).asString should be (string)
      HeaderValue(string).asString should be (string)
      HeaderValue(List("a", "b", "c")).asString should be ("{a,b,c}")
      HeaderValue(Map("key1" -> "v1", "key2" ->"v2")).asString should be ("{key1 = v1,key2 = v2}")
      HeaderValue(true).asString should be ("true")
      HeaderValue(5).asString should be ("5")
    }

    it("Converts scala types to java types for serialization prep") {
      val javaMap = HeaderValue(Map("key1" -> 1)).serializable
      javaMap.isInstanceOf[java.util.Map[_, _]] should be (true)
      javaMap.asInstanceOf[java.util.Map[_, _]].get("key1") should be (1)
    }
  }
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
