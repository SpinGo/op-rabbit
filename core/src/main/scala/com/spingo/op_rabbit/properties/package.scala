package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.BasicProperties.Builder
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.LongString
import scala.collection.JavaConversions._
import java.nio.charset.Charset
import java.util.Date

package object properties {
  type HeaderMap = java.util.HashMap[String, Object]
  trait MessageProperty extends ((Builder, HeaderMap) => Unit)

  trait PropertyExtractor[T] {
    def extractorName: String = getClass.getSimpleName.replace("$", "")
    def unapply(properties: BasicProperties): Option[T]
  }

  sealed trait HeaderValue {
    def value: Any
    def serializable: Object
    def asString(sourceCharset: Charset): String =
      value.toString
    def asString: String =
      asString(Charset.defaultCharset)
  }
  type Converter[T] = T => HeaderValue
  case class LongStringHeaderValue(value: LongString) extends HeaderValue {
    def serializable = value
    override def asString(sourceCharset: Charset) =
      new String(value.getBytes(), sourceCharset)
  }
  case class StringHeaderValue(value: String) extends HeaderValue {
    def serializable = value
    override def asString(sourceCharset: Charset) =
      value
  }
  case class IntHeaderValue(value: Int) extends HeaderValue {
    def serializable = value.asInstanceOf[Integer]
  }
  case class BigDecimalHeaderValue(value: BigDecimal) extends HeaderValue {
    val serializable = value.bigDecimal
    if (serializable.unscaledValue.bitLength() > 32)
      throw new IllegalArgumentException("BigDecimal too large to be encoded");
  }
  case class DateHeaderValue(value: Date) extends HeaderValue {
    val serializable = value
  }
  case class MapHeaderValue(value: Map[String, HeaderValue]) extends HeaderValue {
    lazy val serializable = mapAsJavaMap(value.mapValues(_.serializable))
    override def asString(sourceCharset: Charset) = {
      val b = new StringBuilder()
      b += '{'
      for { (key, value) <- value } {
        b ++= s"${key} = ${value.asString(sourceCharset)}"
        b += ','
      }
      b.deleteCharAt(b.length - 1)
      b += '}'
      b.toString
    }
  }

  case class ByteHeaderValue(value: Byte) extends HeaderValue {
    def serializable = value.asInstanceOf[java.lang.Byte]
  }
  case class DoubleHeaderValue(value: Double) extends HeaderValue {
    def serializable = value.asInstanceOf[java.lang.Double]
  }
  case class FloatHeaderValue(value: Float) extends HeaderValue {
    def serializable = value.asInstanceOf[java.lang.Float]
  }
  case class LongHeaderValue(value: Long) extends HeaderValue {
    def serializable = value.asInstanceOf[java.lang.Long]
  }
  case class ShortHeaderValue(value: Short) extends HeaderValue {
    def serializable = value.asInstanceOf[java.lang.Short]
  }
  case class BooleanHeaderValue(value: Boolean) extends HeaderValue {
    def serializable = value.asInstanceOf[java.lang.Boolean]
  }
  case class ByteArrayHeaderValue(value: Array[Byte]) extends HeaderValue {
    def serializable = value
    override def asString(sourceCharset: Charset) = {
      new String(value, sourceCharset)
    }
  }
  case object NullHeaderValue extends HeaderValue {
    def value = null
    def serializable = null
    override def asString(sourceCharset: Charset) = "null"
  }
  case class SeqHeaderValue[T](value: Seq[HeaderValue]) extends HeaderValue {
    def serializable = value.map(_.serializable)
    override def asString(sourceCharset: Charset) = {
      val b = new StringBuilder()
      b += '{'
      for { (item) <- value } {
        b ++= s"${item.asString(sourceCharset)}"
        b += ','
      }
      b.deleteCharAt(b.length - 1)
      b += '}'
      b.toString
    }
  }
  object HeaderValue {
    implicit val convertFromLongString: Converter[LongString]                                           = LongStringHeaderValue(_)
    implicit val convertFromString: Converter[String]                                                   = StringHeaderValue(_)
    implicit val convertFromInt: Converter[Int]                                                         = { i => IntHeaderValue(i) }
    implicit val convertFromInteger: Converter[Integer]                                                 = { i => IntHeaderValue(i) }
    implicit val convertFromBigDecimal: Converter[BigDecimal]                                           = BigDecimalHeaderValue(_)
    implicit val convertFromJavaBigDecimal: Converter[java.math.BigDecimal]                             = { bd => BigDecimalHeaderValue(BigDecimal(bd)) }
    implicit val convertFromDate: Converter[Date]                                                       = DateHeaderValue(_)
    implicit def convertFromMap[T](implicit converter: Converter[T]): Converter[Map[String, T]]         = { m => MapHeaderValue(m.mapValues(converter)) }
    implicit val convertFromByte: Converter[Byte]                                                       = ByteHeaderValue(_)
    implicit val convertFromJavaByte: Converter[java.lang.Byte]                                         = ByteHeaderValue(_)
    implicit val convertFromDouble: Converter[Double]                                                   = DoubleHeaderValue(_)
    implicit val convertFromJavaDouble: Converter[java.lang.Double]                                     = DoubleHeaderValue(_)
    implicit val convertFromFloat: Converter[Float]                                                     = FloatHeaderValue(_)
    implicit val convertFromJavaFloat: Converter[java.lang.Float]                                       = FloatHeaderValue(_)
    implicit val convertFromLong: Converter[Long]                                                       = LongHeaderValue(_)
    implicit val convertFromJavaLong: Converter[java.lang.Long]                                         = LongHeaderValue(_)
    implicit val convertFromShort: Converter[Short]                                                     = ShortHeaderValue(_)
    implicit val convertFromJavaShort: Converter[java.lang.Short]                                       = ShortHeaderValue(_)
    implicit val convertFromBoolean: Converter[Boolean]                                                 = BooleanHeaderValue(_)
    implicit val convertFromJavaBoolean: Converter[java.lang.Boolean]                                   = BooleanHeaderValue(_)
    implicit val convertFromByteArray: Converter[Array[Byte]]                                           = ByteArrayHeaderValue(_)
    implicit def convertFromSeq[T](implicit converter: Converter[T]): Converter[Seq[T]]                 = { s => SeqHeaderValue(s.map(converter)) }
    implicit def convertFromJavaList[T](implicit converter: Converter[T]): Converter[java.util.List[T]] = { list => SeqHeaderValue(list.map(converter)) }

    def apply[T](value: T)(implicit converter: Converter[T]): HeaderValue =
      if (value == null) NullHeaderValue else converter(value)

    def from(value: Object): HeaderValue = value match {
      case v: String                               => apply(v)
      case v: LongString                           => apply(v)
      case v: Integer                              => apply(v)
      case v: java.math.BigDecimal                 => apply(v)
      case v: java.util.Date                       => apply(v)
      case v: java.util.Map[_, _] =>
        MapHeaderValue(v.map { case (k, v: Object) => (k.toString, from(v)) }.toMap)
      case v: java.lang.Byte                       => apply(v)
      case v: java.lang.Double                     => apply(v)
      case v: java.lang.Float                      => apply(v)
      case v: java.lang.Long                       => apply(v)
      case v: java.lang.Short                      => apply(v)
      case v: java.lang.Boolean                    => apply(v)
      case v: Array[Byte]                          => apply(v)
      case null                                    => NullHeaderValue
      case v: java.util.List[_] =>
        SeqHeaderValue(v.map { case v: Object => from(v) })
      case v: Array[Object] =>
        SeqHeaderValue(v.map(from))
    }
  }

  case class UnboundHeader(name: String) extends (HeaderValue => Header) with PropertyExtractor[HeaderValue] {
    override val extractorName = s"Header(${name})"
    def unapply(properties: BasicProperties) =
      for {
        h <- Option(properties.getHeaders)
        v <- Option(h.get(name))
      } yield HeaderValue.from(v)

    def apply(value: HeaderValue) = Header(extractorName, value)
  }

  class Header protected (name: String, value: HeaderValue) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      headers.put(name, value.serializable)
    }
  }
  object Header {
    def apply(name: String, value: HeaderValue): Header = {
      if (value == null)
        new Header(name, NullHeaderValue)
      else
        new Header(name, value)
    }
    def apply(headerName: String) = UnboundHeader(headerName)
  }
  case class DeliveryMode(mode: Int) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.deliveryMode(mode)
    }
  }
  object DeliveryMode extends PropertyExtractor[Int]{
    def unapply(properties: BasicProperties) =
      Option(properties.getDeliveryMode)
    val nonPersistent = DeliveryMode(1)
    val persistent = DeliveryMode(2)
  }

  case class Priority(priority: Int) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit =
      builder.priority(priority)
  }
  object Priority extends PropertyExtractor[Int] {
    def unapply(properties: BasicProperties) =
      Option(properties.getPriority).map(_.toInt)
  }

  case class CorrelationId(id: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit =
      builder.correlationId(id)
  }
  object CorrelationId extends PropertyExtractor[String] {
    def unapply(properties: BasicProperties) =
      Option(properties.getCorrelationId)
  }

  //   String correlationId,
  case class ReplyTo(replyTo: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit =
      builder.replyTo(replyTo)
  }
  object ReplyTo extends PropertyExtractor[String] {
    def unapply(properties: BasicProperties) =
      Option(properties.getReplyTo)
  }

  case class Expiration(expiration: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.expiration(expiration)
    }
  }
  object Expiration extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getExpiration)
  }

  case class MessageId(messageId: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.messageId(messageId)
    }
  }
  object MessageId extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getMessageId)
  }

  case class Timestamp(timestamp: Date) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.timestamp(timestamp)
    }
  }
  object Timestamp extends PropertyExtractor[Date]{
    def unapply(properties: BasicProperties) =
      Option(properties.getTimestamp)
  }

  case class Type(`type`: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.`type`(`type`)
    }
  }
  object Type extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getType)
  }

  case class UserId(userId: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.userId(userId)
    }
  }
  object UserId extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getUserId)
  }

  case class AppId(appId: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.appId(appId)
    }
  }
  object AppId extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getAppId)
  }

  case class ClusterId(clusterId: String) extends MessageProperty {
    def apply(builder: Builder, headers: HeaderMap): Unit = {
      builder.clusterId(clusterId)
    }
  }
  object ClusterId extends PropertyExtractor[String]{
    def unapply(properties: BasicProperties) =
      Option(properties.getClusterId)
  }

  def builderWithProperties(properties: TraversableOnce[MessageProperty], builder: Builder = new Builder(), headers: HeaderMap = new java.util.HashMap[String, Object]): Builder = {
    properties foreach { p => p(builder, headers) }
    builder.headers(headers)
    builder
  }
}
