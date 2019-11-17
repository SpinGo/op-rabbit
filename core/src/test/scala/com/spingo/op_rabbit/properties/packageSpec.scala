package com.spingo.op_rabbit.properties

import org.scalatest.{FunSpec, Matchers}

import scala.language.postfixOps

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
        DeliveryModePersistence.persistent,
        ReplyTo("reply-destination"),
        Header("header1", "very-value1"),
        Header("header2", "very-value2"))).build
    it("lifts out property values") {
      DeliveryModePersistence.extract(properties) should be (Some(true))

      ReplyTo.extract(properties) should be (Some("reply-destination"))

      Header("header1").extract(properties) should be (Some(HeaderValue("very-value1")))
      Header("header2").extract(properties) should be (Some(HeaderValue("very-value2")))
    }
  }

  describe("UnboundTypedHeaderLongToFiniteDuration") {
    import scala.concurrent.duration._
    val test = UnboundTypedHeaderLongToFiniteDuration("test")

    it("outputs the provided duration to a long") {
      toJavaMap(Seq(test(5 seconds))).get("test") should be (5000L)
    }

    it("reads the same duration back") {
      val boundHeader = test(5 seconds)
      val m = toJavaMap(Seq(boundHeader))
      test.extract(m) should be (Some(5 seconds))
    }
  }
}
