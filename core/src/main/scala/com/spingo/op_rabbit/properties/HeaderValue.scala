package com.spingo.op_rabbit.properties

import java.nio.charset.Charset
import java.util.Date
import com.rabbitmq.client.LongString
import scala.collection.JavaConversions._

/**
  Trait which represents all values allowed in property generc headers

  See the source for [[https://github.com/rabbitmq/rabbitmq-java-client/blob/rabbitmq_v3_5_3/src/com/rabbitmq/client/impl/ValueReader.java#L157 com.rabbitmq.client.impl.ValueReader.readFieldValue]]
  */
sealed trait HeaderValue {
  def value: Any
  def serializable: Object
  def asString(sourceCharset: Charset): String =
    value.toString
  def asString: String =
    asString(Charset.defaultCharset)

  def asOpt[T](implicit conversion: HeaderValueConverter[T]): Option[T] = conversion(this).right.toOption
}
object HeaderValue {
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
