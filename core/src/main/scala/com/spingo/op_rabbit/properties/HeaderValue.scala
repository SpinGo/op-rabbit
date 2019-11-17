package com.spingo.op_rabbit.properties

import java.nio.charset.Charset
import java.util.Date
import com.rabbitmq.client.LongString
import scala.collection.JavaConverters._

/**
  Trait which represents all values allowed in property generc headers

  See the source for [[https://github.com/rabbitmq/rabbitmq-java-client/blob/rabbitmq_v3_5_3/src/com/rabbitmq/client/impl/ValueReader.java#L157 com.rabbitmq.client.impl.ValueReader.readFieldValue]]
  */
sealed trait HeaderValue {
  def value: Any
  def serializable: Object
  def asString(sourceCharset: Charset): String = value.toString
  def asString: String = asString(Charset.defaultCharset)

  def asOpt[T](implicit conversion: FromHeaderValue[T]): Option[T] = conversion(this).right.toOption
}
object HeaderValue {
  case class LongStringHeaderValue(value: LongString) extends HeaderValue {
    def serializable = value
    override def asString(sourceCharset: Charset) =
      new String(value.getBytes, sourceCharset)
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
      throw new IllegalArgumentException("BigDecimal too large to be encoded")
  }
  case class DateHeaderValue(value: Date) extends HeaderValue {
    val serializable = value
  }
  case class MapHeaderValue(value: Map[String, HeaderValue]) extends HeaderValue {
    lazy val serializable = value.mapValues(_.serializable).toMap.asJava
    override def asString(sourceCharset: Charset) = {
      val b = new StringBuilder()
      b += '{'
      for { (key, value) <- value } {
        b ++= s"$key = ${value.asString(sourceCharset)}"
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
  case class SeqHeaderValue(value: Seq[HeaderValue]) extends HeaderValue {
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

  implicit val convertFromLongString     : ToHeaderValue[LongString           , LongStringHeaderValue] = LongStringHeaderValue(_)
  implicit val convertFromString         : ToHeaderValue[String               , StringHeaderValue]     = StringHeaderValue(_)
  implicit val convertFromInt            : ToHeaderValue[Int                  , IntHeaderValue]        = { i => IntHeaderValue(i) }
  implicit val convertFromInteger        : ToHeaderValue[Integer              , IntHeaderValue]        = { i => IntHeaderValue(i) }
  implicit val convertFromBigDecimal     : ToHeaderValue[BigDecimal           , BigDecimalHeaderValue] = BigDecimalHeaderValue(_)
  implicit val convertFromJavaBigDecimal : ToHeaderValue[java.math.BigDecimal , BigDecimalHeaderValue] = { bd => BigDecimalHeaderValue(BigDecimal(bd)) }
  implicit val convertFromDate           : ToHeaderValue[Date                 , DateHeaderValue]       = DateHeaderValue(_)
  implicit val convertFromByte           : ToHeaderValue[Byte                 , ByteHeaderValue]       = ByteHeaderValue(_)
  implicit val convertFromJavaByte       : ToHeaderValue[java.lang.Byte       , ByteHeaderValue]       = ByteHeaderValue(_)
  implicit val convertFromDouble         : ToHeaderValue[Double               , DoubleHeaderValue]     = DoubleHeaderValue(_)
  implicit val convertFromJavaDouble     : ToHeaderValue[java.lang.Double     , DoubleHeaderValue]     = DoubleHeaderValue(_)
  implicit val convertFromFloat          : ToHeaderValue[Float                , FloatHeaderValue]      = FloatHeaderValue(_)
  implicit val convertFromJavaFloat      : ToHeaderValue[java.lang.Float      , FloatHeaderValue]      = FloatHeaderValue(_)
  implicit val convertFromLong           : ToHeaderValue[Long                 , LongHeaderValue]       = LongHeaderValue(_)
  implicit val convertFromJavaLong       : ToHeaderValue[java.lang.Long       , LongHeaderValue]       = LongHeaderValue(_)
  implicit val convertFromShort          : ToHeaderValue[Short                , ShortHeaderValue]      = ShortHeaderValue(_)
  implicit val convertFromJavaShort      : ToHeaderValue[java.lang.Short      , ShortHeaderValue]      = ShortHeaderValue(_)
  implicit val convertFromBoolean        : ToHeaderValue[Boolean              , BooleanHeaderValue]    = BooleanHeaderValue(_)
  implicit val convertFromJavaBoolean    : ToHeaderValue[java.lang.Boolean    , BooleanHeaderValue]    = BooleanHeaderValue(_)
  implicit val convertFromByteArray      : ToHeaderValue[Array[Byte]          , ByteArrayHeaderValue]  = ByteArrayHeaderValue(_)
  implicit def convertFromMap[T](implicit converter: ToHeaderValue[T, HeaderValue]): ToHeaderValue[Map[String, T], MapHeaderValue]         = { m => MapHeaderValue(m.mapValues(converter).toMap) }
  implicit def convertFromSeq[T](implicit converter: ToHeaderValue[T, HeaderValue]): ToHeaderValue[Seq[T], SeqHeaderValue]                 = { s => SeqHeaderValue(s.map(converter)) }
  implicit def convertFromJavaList[T](implicit converter: ToHeaderValue[T, HeaderValue]): ToHeaderValue[java.util.List[T], SeqHeaderValue] = { list => SeqHeaderValue(list.asScala.toSeq.map(converter)) }

  def apply[T](value: T)(implicit converter: ToHeaderValue[T, HeaderValue]): HeaderValue =
    if (value == null) NullHeaderValue else converter(value)

  def from(value: Object): HeaderValue = value match {
    case v: String                               => apply(v)
    case v: LongString                           => apply(v)
    case v: Integer                              => apply(v)
    case v: java.math.BigDecimal                 => apply(v)
    case v: java.util.Date                       => apply(v)
    case v: java.util.Map[_, _] =>
      MapHeaderValue(v.asScala.map {
        case (k, v: Object) => (k.toString, from(v))
        case (k, otherwise) =>
          throw new RuntimeException(
            s"RabbitMQ Java driver gave us a value we did not expect ${otherwise.getClass}. We should never get here.")
      }.toMap)
    case v: java.lang.Byte                       => apply(v)
    case v: java.lang.Double                     => apply(v)
    case v: java.lang.Float                      => apply(v)
    case v: java.lang.Long                       => apply(v)
    case v: java.lang.Short                      => apply(v)
    case v: java.lang.Boolean                    => apply(v)
    case v: Array[Byte]                          => apply(v)
    case null                                    => NullHeaderValue
    case v: java.util.List[_] =>
      SeqHeaderValue(v.asScala.toSeq.map { case v: Object => from(v) })
    case v: Array[Object] =>
      SeqHeaderValue(v.map(from))
    case otherwise =>
      throw new RuntimeException(
        s"RabbitMQ Java driver gave us a value we did not expect ${otherwise.getClass}. We should never get here.")
  }
}
