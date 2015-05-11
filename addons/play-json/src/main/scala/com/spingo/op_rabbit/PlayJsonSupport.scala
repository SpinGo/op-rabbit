package com.spingo.op_rabbit

import play.api.libs.json._

import java.nio.charset.Charset

object PlayJsonSupport {
  private val utf8 = Charset.forName("UTF-8")
  implicit def playJsonRabbitMarshaller[T](implicit writer: Writes[T]): RabbitMarshaller[T] = {
    new RabbitMarshaller[T] {
      val contentType = "application/json"
      val encoding = "UTF-8"
      def marshall(value: T) =
        (Json.stringify(writer.writes(value)).getBytes(utf8), contentType, Some(encoding))
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
