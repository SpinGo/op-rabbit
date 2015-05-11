package com.spingo.op_rabbit

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.charset.Charset
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import scala.util.Try

object StringHelpers {
  import scala.collection.JavaConversions
  def guessCharset(body: Array[Byte]): Option[Charset] = {
    val firstChars: List[Byte] = (0 until Math.min(20, body.length)) map (body(_)) toList

    if (firstChars.exists { b => b < 0 })
      None
    else
      Some(Charset.forName("UTF8"))
  }

  def byteArrayToString(body: Array[Byte], charset: Option[Charset]) = {
    (charset orElse guessCharset(body)) match {
      case Some(c) => Try { new String(body, c) } getOrElse { "<Malencoded-String>" }
      case None => "<Binary-Data>"
    }
  }
}
trait RabbitErrorLogging {
  private val utf8 = Charset.forName("UTF-8")
  def bodyAsString(body: Array[Byte], properties: BasicProperties): String =
    StringHelpers.byteArrayToString(body, Try { Charset.forName(properties.getContentEncoding) } toOption)

  def apply(name: String, message: String, exception: Throwable, consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit

  def +(other: RabbitErrorLogging) = new CombinedLogger(this, other)
}

class CombinedLogger(a: RabbitErrorLogging, b: RabbitErrorLogging) extends RabbitErrorLogging {
  def apply(name: String, message: String, exception: Throwable, consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
    a(name, message, exception, consumerTag, envelope, properties, body)
    b(name, message, exception, consumerTag, envelope, properties, body)
  }
}

object LogbackLogger extends RabbitErrorLogging {
  def apply(name: String, message: String, exception: Throwable, consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
    val logger = LoggerFactory.getLogger(name)
    logger.error(s"${message}. Body=${bodyAsString(body, properties)}. Envelope=${envelope}", exception)
  }
}

object RabbitErrorLogging {
  implicit val defaultLogger: RabbitErrorLogging = LogbackLogger
}
