package com.spingo.op_rabbit

import org.scalatest.FunSpec
import org.scalatest.Matchers
import play.api.libs.json._

class PlayJsonSupportSpec extends FunSpec with Matchers {
  case class Thing(a: Int)
  implicit val thingFormat = Json.format[Thing]
  import PlayJsonSupport._
  val u = implicitly[RabbitUnmarshaller[Thing]]
  val m = implicitly[RabbitMarshaller[Thing]]

  describe("PlayJsonSupport") {
    it("deserializes the provided content") {
      u.unmarshall("""{"a": 5}""".getBytes, Some("application/json"), Some("UTF-8")) should be (Thing(5))
    }

    it("interprets no encoding / no contentType as json / UTF8") {
      u.unmarshall("""{"a": 5}""".getBytes, None, None) should be (Thing(5))
    }

    it("rejects wrong encoding") {
      a [MismatchedContentType] should be thrownBy {
        u.unmarshall("""{"a": 5}""".getBytes, Some("text"), Some("UTF-8"))
      }
    }

    it("throws an InvalidFormat exception when unmarshalling is unpossible") {
      a [InvalidFormat] should be thrownBy {
        u.unmarshall("""{"a": }""".getBytes, Some("application/json"), Some("UTF-8"))
      }
    }

    it("serializes the provided content") {
      val body = m.marshall(Thing(5))
    }

    it("provides the appropriate content headers") {
      val properties = m.setProperties().build
      properties.getContentType should be ("application/json")
      properties.getContentEncoding should be ("UTF-8")
    }
  }
}
