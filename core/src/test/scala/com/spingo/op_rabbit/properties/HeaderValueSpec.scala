package com.spingo.op_rabbit.properties

import org.scalatest.{FunSpec, Matchers}
import com.spingo.op_rabbit.properties._
import com.rabbitmq.client.impl.LongStringHelper
import com.rabbitmq.client.AMQP.BasicProperties.Builder
import com.rabbitmq.client.AMQP.BasicProperties

class HeaderValueSpec extends FunSpec with Matchers {
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

  describe("HeaderValueConverter") {
    def testNumeric[T](what: String, expectedValue: T)(implicit converter: HeaderValueConverter[T]) = {
      it(s"Converts strings or numeric values to Some(${expectedValue})") {
        HeaderValue(LongStringHelper.asLongString("1")).as[T] should be (Some(expectedValue))
        HeaderValue(1).as[T] should be (Some(expectedValue))
        HeaderValue(1L).as[T] should be (Some(expectedValue))
        HeaderValue(1.0f).as[T] should be (Some(expectedValue))
        HeaderValue(1.0d).as[T] should be (Some(expectedValue))
        HeaderValue(1.toByte).as[T] should be (Some(expectedValue))
        HeaderValue(1.toShort).as[T] should be (Some(expectedValue))
      }

      it("Returns None for Byte Arrays or collections") {
        HeaderValue(Seq(1)).as[Int] should be (None)
        HeaderValue(Map("a" -> 1)).as[Int] should be (None)
        HeaderValue("1".getBytes).as[Int] should be (None)
      }
    }

    describe("Int") {
      testNumeric[Int]("String", 1)

      it("Returns None for numbers too large") {
        HeaderValue(0xfffffffffL).as[Int] should be (None)
      }
    }

    describe("Float") {
      testNumeric[Float]("Float", 1.0f)
    }

    describe("Double") {
      testNumeric[Float]("Float", 1.0f)
    }

    describe("BigDecimal") {
      testNumeric[BigDecimal]("BigDecimal", BigDecimal(1))
    }

    describe("Seq") {
      it("extracts a Seq[Int] when all members can be converted") {
        HeaderValue(Seq("1", "2", "3")).as[Seq[Int]] should be (Some(Seq(1,2,3)))
      }

      it("handles nested sequences") {
        HeaderValue(Seq(Seq("1"), Seq("2"), Seq("3"))).as[Seq[Seq[Int]]] should be (Some(Seq(Seq(1),Seq(2),Seq(3))))
      }

      it("returns None when any of the members can't be converted") {
        HeaderValue(Seq("1", "A", "3")).as[Seq[Int]] should be (None)
      }

      it("returns None when the container object is not a Seq") {
        HeaderValue("1,2,3").as[Seq[Int]] should be (None)
      }
    }

    describe("Map") {
      it("extracts a Map[String, Int] when all members can be converted") {
        HeaderValue(Map("a" -> "1", "b" -> "2", "c" -> "3")).as[Map[String, Int]] should be (Some(Map("a" -> 1, "b" -> 2, "c" -> 3)))
      }

      it("returns None when any of the members can't be converted") {
        HeaderValue(Map("a" -> "1", "b" -> "a", "c" -> "3")).as[Map[String, Int]] should be (None)
      }

      it("returns None when the container object is not a Seq") {
        HeaderValue("1,2,3").as[Map[String, Int]] should be (None)
      }
    }
  }
}
