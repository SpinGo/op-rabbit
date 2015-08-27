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

case class HeaderValueConversionException(msg: String, cause: Throwable = null) extends Exception(msg, cause)
trait HeaderValueConverter[T] { def apply(hv: HeaderValue): Either[HeaderValueConversionException, T] }

case class StringHeaderValueConverter(charset: Charset = Charset.defaultCharset) extends HeaderValueConverter[String] {
  def apply(hv: HeaderValue) = {
    Right(hv.asString(charset))
  }
}

object NumericValueConversion {
  def apply[T](hv: HeaderValue)(inner: PartialFunction[HeaderValue, Either[HeaderValueConversionException, T]])(implicit mf: Manifest[T]): Either[HeaderValueConversionException, T] = try {
    val thing: PartialFunction[HeaderValue, Either[HeaderValueConversionException, T]] = { case _ => Left(HeaderValueConversionException(s"Could not convert ${hv} to ${mf.getClass}")) }
    (inner orElse thing)(hv)
  } catch {
    case ex: java.lang.NumberFormatException => Left(HeaderValueConversionException(s"Could not convert ${hv} to ${mf.getClass}", ex))
  }

}

case class IntHeaderValueConverter(charset: Charset = Charset.defaultCharset) extends HeaderValueConverter[Int] {
  def apply(hv: HeaderValue) = NumericValueConversion[Int](hv) {
    case v: LongStringHeaderValue => Right(v.asString(charset).toInt)
    case StringHeaderValue(v)     => Right(v.toInt)
    case IntHeaderValue(v)        => Right(v)
    case ByteHeaderValue(v)       => Right(v.toInt)
    case ShortHeaderValue(v)      => Right(v.toInt)
    case BigDecimalHeaderValue(v) if (v >= Int.MinValue && v <= Int.MaxValue) => Right(v.toInt)
    case DoubleHeaderValue(v)     if (v >= Int.MinValue && v <= Int.MaxValue) => Right(v.toInt)
    case FloatHeaderValue(v)      if (v >= Int.MinValue && v <= Int.MaxValue) => Right(v.toInt)
    case LongHeaderValue(v)       if (v >= Int.MinValue && v <= Int.MaxValue) => Right(v.toInt)
  }
}

case class FloatHeaderValueConverter(charset: Charset = Charset.defaultCharset) extends HeaderValueConverter[Float] {
  def apply(hv: HeaderValue) =  NumericValueConversion[Float](hv) {
    case v: LongStringHeaderValue => Right(v.asString(charset).toFloat)
    case StringHeaderValue(v)     => Right(v.toFloat)
    case IntHeaderValue(v)        => Right(v)
    case ByteHeaderValue(v)       => Right(v.toFloat)
    case DoubleHeaderValue(v)     => Right(v.toFloat)
    case FloatHeaderValue(v)      => Right(v.toFloat)
    case LongHeaderValue(v)       => Right(v.toFloat)
    case ShortHeaderValue(v)      => Right(v.toFloat)
    case BigDecimalHeaderValue(v) => Right(v.toFloat)
  }
}

case class DoubleHeaderValueConverter(charset: Charset = Charset.defaultCharset) extends HeaderValueConverter[Double] {
  def apply(hv: HeaderValue) = NumericValueConversion[Double](hv) {
    case v: LongStringHeaderValue => Right(v.asString(charset).toDouble)
    case StringHeaderValue(v)     => Right(v.toDouble)
    case IntHeaderValue(v)        => Right(v)
    case ByteHeaderValue(v)       => Right(v.toDouble)
    case DoubleHeaderValue(v)     => Right(v.toDouble)
    case FloatHeaderValue(v)      => Right(v.toDouble)
    case LongHeaderValue(v)       => Right(v.toDouble)
    case ShortHeaderValue(v)      => Right(v.toDouble)
    case BigDecimalHeaderValue(v) => Right(v.toDouble)
  }
}

case class BigDecimalHeaderValueConverter(charset: Charset = Charset.defaultCharset) extends HeaderValueConverter[BigDecimal] {
  def apply(hv: HeaderValue) = NumericValueConversion[BigDecimal](hv) {
    case v: LongStringHeaderValue => Right(BigDecimal(v.asString(charset)))
    case StringHeaderValue(v)     => Right(BigDecimal(v))
    case IntHeaderValue(v)        => Right(BigDecimal(v))
    case ByteHeaderValue(v)       => Right(BigDecimal(v))
    case DoubleHeaderValue(v)     => Right(BigDecimal(v))
    case FloatHeaderValue(v)      => Right(BigDecimal(v))
    case LongHeaderValue(v)       => Right(BigDecimal(v))
    case ShortHeaderValue(v)      => Right(BigDecimal(v))
    case BigDecimalHeaderValue(v) => Right(v)
  }
}

case class LongHeaderValueConverter(charset: Charset = Charset.defaultCharset) extends HeaderValueConverter[Long] {
  def apply(hv: HeaderValue) = NumericValueConversion[Long](hv) {
    case v: LongStringHeaderValue => Right(v.asString(charset).toLong)
    case StringHeaderValue(v)     => Right(v.toLong)
    case IntHeaderValue(v)        => Right(v.toLong)
    case ByteHeaderValue(v)       => Right(v.toLong)
    case LongHeaderValue(v)       => Right(v)
    case ShortHeaderValue(v)      => Right(v.toLong)
    case DoubleHeaderValue(v)     if (v <= Long.MaxValue && v >= Long.MinValue) => Right(v.toLong)
    case FloatHeaderValue(v)      if (v <= Long.MaxValue && v >= Long.MinValue) => Right(v.toLong)
    case BigDecimalHeaderValue(v) if (v >= Long.MinValue && v <= Long.MaxValue) => Right(v.toLong)
  }
}

case class SeqHeaderValueConverter[T](charset: Charset = Charset.defaultCharset)(implicit subConverter: HeaderValueConverter[T]) extends HeaderValueConverter[Seq[T]] {
  def apply(hv: HeaderValue): Either[HeaderValueConversionException, Seq[T]] = hv match {
    case SeqHeaderValue(seq) => {
      val builder = Seq.newBuilder[T]
      seq foreach { elem =>
        builder += (subConverter(elem) match {
          case Right(v) => v
          case Left(ex) => return Left(HeaderValueConversionException("One or more elements could not be converted", ex))
        })
      }
      Right(builder.result)
    }
    case _ =>
      Left(HeaderValueConversionException("${hv} is not a Seq"))
  }
}

case class MapHeaderValueConverter[T](charset: Charset = Charset.defaultCharset)(implicit subConverter: HeaderValueConverter[T]) extends HeaderValueConverter[Map[String, T]] {
  def apply(hv: HeaderValue): Either[HeaderValueConversionException, Map[String, T]] = hv match {
    case MapHeaderValue(map) => {
      val builder = Map.newBuilder[String, T]
      map foreach { case (key, value) =>
        builder += key -> (subConverter(value) match {
          case Right(v) => v
          case Left(ex) => return Left(HeaderValueConversionException("One or more elements could not be converted", ex))
        })
      }
      Right(builder.result)
    }
    case _ =>
      Left(HeaderValueConversionException("${hv} is not a Map"))
  }
}

object HeaderValueConverter {
  implicit val defaultStringConversion = StringHeaderValueConverter()
  implicit val defaultIntConversion = IntHeaderValueConverter()
  implicit val defaultDoubleConversion = DoubleHeaderValueConverter()
  implicit val defaultLongConversion = LongHeaderValueConverter()
  implicit val defaultFloatConversion = FloatHeaderValueConverter()
  implicit val defaultBigDecimalConversion = BigDecimalHeaderValueConverter()
  implicit def defaultSeqHeaderValueConverter[T](implicit subConverter: HeaderValueConverter[T]) = SeqHeaderValueConverter[T]()
  implicit def defaultMapHeaderValueConverter[T](implicit subConverter: HeaderValueConverter[T]) = MapHeaderValueConverter[T]()
}
