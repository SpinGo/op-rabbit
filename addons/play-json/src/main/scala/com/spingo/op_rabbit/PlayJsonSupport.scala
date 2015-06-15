package com.spingo.op_rabbit

import play.api.libs.json._

import java.nio.charset.Charset

/**
  Use implicit PlayJson formats for serialization by importing this object.

  Example:

  {{{
  
  import play.api.libs.json._
  import com.spingo.op_rabbit.PlayJsonSupport._

  object Example {
    case class Person(name: String, age: Int)
    implicit val format = Json.format[Person]
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
object PlayJsonSupport {
  private val utf8 = Charset.forName("UTF-8")
  implicit def playJsonRabbitMarshaller[T](implicit writer: Writes[T]): RabbitMarshaller[T] = {
    new RabbitMarshaller[T] {
      protected val contentType = "application/json"
      private val encoding = "UTF-8"
      protected val contentEncoding = Some(encoding)
      def marshall(value: T) =
        Json.stringify(writer.writes(value)).getBytes(utf8)
    }
  }
  implicit def playJsonRabbitUnmarshaller[T](implicit reads: Reads[T]): RabbitUnmarshaller[T] = {
    new RabbitUnmarshaller[T] {
      def unmarshall(value: Array[Byte], contentType: Option[String], charset: Option[String]): T = {
        contentType match {
          case Some(value) if (value != "application/json" && value != "text/json") =>
            throw MismatchedContentType(value, "application/json")
          case _ =>
            reads.reads(Json.parse(new String(value, charset map (Charset.forName) getOrElse utf8))).get
        }
      }
    }
  }
}
