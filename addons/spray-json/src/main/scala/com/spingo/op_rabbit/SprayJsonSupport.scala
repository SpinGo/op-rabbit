package com.spingo.op_rabbit

import spray.json._
import java.nio.charset.Charset

/**
  == BATTERIES NOT INCLUDED ==

  To use this package, you must add `'op-rabbit-spray-json'` to your dependencies.

  == Overview ==

  Use implicit SprayJson formats for serialization by importing this object.

  Example:

  {{{
  
  import spray.json.DefaultJsonProtocol
  import com.spingo.op_rabbit.SprayJsonSupport._

  object Example extends DefaultJsonProtocol {
    case class Person(name: String, age: Int)
    implicit val format = jsonFormat2(Person)
    // Both of these can be implicitly created:
    // - implicitly[RabbitMarshaller[Person]]
    // - implicitly[RabbitUnmarshaller[Person]]
    val consumer = AsyncAckingConsumer[Person]("PurplePeopleEater") { person =>
      Future { eat(person) }
    }
    val message = QueueMessage(Person("Bill", 25), "people-for-consumption")
  }

  }}}
  */
object SprayJsonSupport {
  private val utf8 = Charset.forName("UTF-8")
  implicit def sprayJsonRabbitMarshaller[T](implicit writer : spray.json.JsonWriter[T]): RabbitMarshaller[T] = {
    new RabbitMarshaller[T] {
      protected val contentType = "application/json"
      private val encoding = "UTF-8"
      protected val contentEncoding = Some(encoding)
      def marshall(value: T) =
        value.toJson.prettyPrint.getBytes(utf8)
    }
  }
  implicit def sprayJsonRabbitUnmarshaller[T](implicit reads: spray.json.JsonReader[T]): RabbitUnmarshaller[T] = {
    new RabbitUnmarshaller[T] {
      def unmarshall(value: Array[Byte], contentType: Option[String], charset: Option[String]): T = {
        contentType match {
          case Some(x) if x != "application/json" && x != "text/json" =>
            throw MismatchedContentType(x, "application/json")
          case _ =>
            new String(value, charset map Charset.forName getOrElse utf8).parseJson.convertTo[T]
        }
      }
    }
  }
}
