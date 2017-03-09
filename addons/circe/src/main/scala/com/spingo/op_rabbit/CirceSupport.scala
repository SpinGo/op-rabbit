package com.spingo.op_rabbit

import io.circe.{Decoder, Encoder}
import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import java.nio.charset.Charset

object CirceSupport {
  private val utf8 = Charset.forName("UTF-8")
  implicit def circeRabbitMarshaller[T](implicit encoder: Encoder[T]): RabbitMarshaller[T] = {
    new RabbitMarshaller[T] {
      protected val contentType = "application/json"
      private val encoding = "UTF-8"
      protected val contentEncoding = Some(encoding)
      def marshall(value: T) =
        value.asJson.noSpaces.getBytes(utf8)
    }
  }

  implicit def circeRabbitUnmarshaller[T](implicit decoder: Decoder[T]): RabbitUnmarshaller[T] = {
    new RabbitUnmarshaller[T] {
      def unmarshall(value: Array[Byte], contentType: Option[String], charset: Option[String]): T = {
        contentType match {
          case Some(value) if (value != "application/json" && value != "text/json") =>
            throw MismatchedContentType(value, "application/json")
          case _ =>
            val str = try {
              new String(
                value,
                charset map (Charset.forName) getOrElse utf8)
            } catch {
              case ex: Throwable =>
                throw new GenericMarshallingException(
                  s"Could not convert input to charset of type ${charset}; ${ex.toString}")
            }

            decode[T](str) match {
              case Left(error) =>
                throw InvalidFormat(str, error.getMessage)
              case Right(v) => v
            }
        }
      }
    }
  }
}
