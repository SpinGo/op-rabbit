package com.spingo.op_rabbit

import org.scalatest.FunSpec
import org.scalatest.Matchers

case class Thing(a: Int)

class Json4sSupportSpec extends FunSpec with Matchers {
  import Json4sSupport._
  implicit val formats = org.json4s.DefaultFormats
  val u = implicitly[RabbitUnmarshaller[Thing]]
  val m = implicitly[RabbitMarshaller[Thing]]

  describe("Json4sSupport") {
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

    it("serializes the provided content") {
      val body = m.marshall(Thing(5))
      new String(body) should be ("""{"a":5}""")
    }

    it("provides the appropriate content headers") {
      val properties = m.properties().build
      properties.getContentType should be ("application/json")
      properties.getContentEncoding should be ("UTF-8")
    }
  }
}
