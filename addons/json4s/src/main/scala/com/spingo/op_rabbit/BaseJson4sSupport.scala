package com.spingo.op_rabbit

import org.json4s._
import java.nio.charset.Charset

/**
  Base Json4sSupport; since there are two serialization drivers for
  Json4s, this trait has two implementations, one providing the native
  serialization driver, the other the jackson
  */
trait BaseJson4sSupport {
  val serialization: org.json4s.Serialization

  private val utf8 = Charset.forName("UTF-8")
  implicit def json4sRabbitMarshaller[T <: AnyRef](implicit formats: Formats): RabbitMarshaller[T] = {
    new RabbitMarshaller[T] {
      protected val contentType = "application/json"
      private val encoding = "UTF-8"
      protected val contentEncoding = Some(encoding)
      def marshall(value: T) =
        serialization.write(value).toString.getBytes(utf8)
    }
  }

  implicit def json4sRabbitUnmarshaller[T <: AnyRef](implicit formats: Formats, manifest: Manifest[T]): RabbitUnmarshaller[T] = {
    new RabbitUnmarshaller[T] {
      def unmarshall(value: Array[Byte], contentType: Option[String], charset: Option[String]): T = {
        contentType match {
          case Some(value) if (value != "application/json" && value != "text/json") =>
            throw MismatchedContentType(value, "application/json")
          case _ =>
            serialization.read[T](new String(value, charset map (Charset.forName) getOrElse utf8))
        }
      }
    }
  }
}
