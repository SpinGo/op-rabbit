package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.BasicProperties
import java.nio.charset.Charset

case class MismatchedContentType(received: String, expected: String) extends Exception(s"MismatchedContentType: expected '${expected}', received '${received}'")

trait RabbitMarshaller[T] {
  def marshall(value: T): (Array[Byte], String, Option[String])
  def marshallWithProperties(value: T, properties: BasicProperties.Builder = new BasicProperties.Builder()): (Array[Byte], BasicProperties.Builder) = {
    val (result, contentType, contentEncoding) = marshall(value)
    val p = properties.contentType(contentType)
    (result, contentEncoding map (p.contentEncoding) getOrElse p)
  }
}

trait RabbitUnmarshaller[T] {
  def unmarshall(value: Array[Byte], contentType: Option[String], contentEncoding: Option[String]): T
}

object BinaryMarshaller extends RabbitMarshaller[Array[Byte]] with RabbitUnmarshaller[Array[Byte]] {
  val contentType = "application/octet-stream"
  def marshall(value: Array[Byte]) = (value, contentType, None)
  def unmarshall(value: Array[Byte], contentType: Option[String], charset: Option[String]): Array[Byte] = value
}

object UTF8StringMarshaller extends RabbitMarshaller[String] with RabbitUnmarshaller[String] {
  val contentType = "text/plain"
  val encoding = "UTF-8"
  private val utf8 = Charset.forName(encoding)
  def marshall(value: String) =
    (value.getBytes(utf8), contentType, Some(encoding))

  def unmarshall(value: Array[Byte], contentType: Option[String], charset: Option[String]) = {
    new String(value, charset map (Charset.forName) getOrElse utf8)
  }
}

object DefaultMarshalling {
  implicit val binaryMarshaller = BinaryMarshaller
  implicit val utf8StringMarshaller = UTF8StringMarshaller
}
