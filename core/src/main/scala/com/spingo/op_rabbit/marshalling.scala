package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.BasicProperties
import java.nio.charset.Charset

/**
  This exception is thrown when a [[RabbitUnmarshaller]] tries to
  unmarshall a message with the wrong contentType specified in the
  header.
  */
case class MismatchedContentType(received: String, expected: String) extends Exception(s"MismatchedContentType: expected '${expected}', received '${received}'")

/**
  This trait is used to serialize messages for publication; it
  configures a property builder and sets the appropriate headers

  @see [[RabbitUnmarshaller]], [[DefaultMarshalling$ DefaultMarshalling]]
  */
trait RabbitMarshaller[T] {
  /**
    Given a value, returns a tuple of:

    - The serialized value
    - A string representing the contentType (e.g. "application/json")
    - An optional string representing the text encoding (e.g. "UTF-8")
    */
  def marshall(value: T): (Array[Byte], String, Option[String])

  /**
    Given a value, and an optional property builder, returns the
    marshalled value and the property builder.

    Note, that if a property builder is provided, it is mutated by
    this method.
    */
  def marshallWithProperties(value: T, properties: BasicProperties.Builder = new BasicProperties.Builder()): (Array[Byte], BasicProperties.Builder) = {
    val (result, contentType, contentEncoding) = marshall(value)
    properties.contentType(contentType)
    contentEncoding.foreach(properties.contentEncoding(_))
    (result, properties)
  }
}

/**
  This trait is used to deserialize messages from binary format for
  use in Consumers; it checks and honors the contentType / encoding
  message headers, as appropriate.
  
  @see [[RabbitMarshaller]], [[DefaultMarshalling$ DefaultMarshalling]]
  */
trait RabbitUnmarshaller[T] {
  /**
    @throws MismatchedContentType
    */
  def unmarshall(value: Array[Byte], contentType: Option[String], contentEncoding: Option[String]): T
}

/**
  Pull binary message payload raw, without any serialization. An implicit is defined in [[DefaultMarshalling.binaryMarshaller]]
  */
object BinaryMarshaller extends RabbitMarshaller[Array[Byte]] with RabbitUnmarshaller[Array[Byte]] {
  val contentType = "application/octet-stream"
  def marshall(value: Array[Byte]) = (value, contentType, None)
  def unmarshall(value: Array[Byte], contentType: Option[String], charset: Option[String]): Array[Byte] = value
}

/**
  Converts binary message to a UTF8 string, and back. An implicit is defined in [[DefaultMarshalling.utf8StringMarshaller]]
  */
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
