package com.spingo.op_rabbit.properties

import com.rabbitmq.client.LongString
import java.nio.charset.Charset
import java.util.Date
import scala.collection.JavaConversions._

/**
  Trait which describes converters that convert a [[HeaderValue]] from one type to another.
  */
trait FromHeaderValue[T] { self =>
  val manifest: Manifest[T]
  def apply(hv: HeaderValue): Either[FromHeaderValue.HeaderValueConversionException, T]

  def map[U : Manifest](fn: T => U) = new FromHeaderValue[U] {
    val manifest = implicitly[Manifest[U]]
    def apply(hv: HeaderValue): Either[FromHeaderValue.HeaderValueConversionException, U] =
      self(hv).right.map(fn)
  }
}

object FromHeaderValue {
  import HeaderValue._
  case class HeaderValueConversionException(msg: String, cause: Throwable = null) extends Exception(msg, cause)

  case class StringFromHeaderValue(charset: Charset = Charset.defaultCharset) extends FromHeaderValue[String] {
    val manifest = implicitly[Manifest[String]]
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

  case class ByteFromHeaderValue(charset: Charset = Charset.defaultCharset) extends FromHeaderValue[Byte] {
    val manifest = implicitly[Manifest[Byte]]
    def apply(hv: HeaderValue) = NumericValueConversion[Byte](hv) {
      case v: LongStringHeaderValue => Right(v.asString(charset).toByte)
      case StringHeaderValue(v)     => Right(v.toByte)
      case ByteHeaderValue(v)       => Right(v)
      case IntHeaderValue(v)        if (v >= Byte.MinValue && v <= Byte.MaxValue) => Right(v.toByte)
      case ShortHeaderValue(v)      if (v >= Byte.MinValue && v <= Byte.MaxValue) => Right(v.toByte)
      case BigDecimalHeaderValue(v) if (v >= Byte.MinValue && v <= Byte.MaxValue) => Right(v.toByte)
      case DoubleHeaderValue(v)     if (v >= Byte.MinValue && v <= Byte.MaxValue) => Right(v.toByte)
      case FloatHeaderValue(v)      if (v >= Byte.MinValue && v <= Byte.MaxValue) => Right(v.toByte)
      case LongHeaderValue(v)       if (v >= Byte.MinValue && v <= Byte.MaxValue) => Right(v.toByte)
    }
  }

  case class IntFromHeaderValue(charset: Charset = Charset.defaultCharset) extends FromHeaderValue[Int] {
    val manifest = implicitly[Manifest[Int]]
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

  case class FloatFromHeaderValue(charset: Charset = Charset.defaultCharset) extends FromHeaderValue[Float] {
    val manifest = implicitly[Manifest[Float]]
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

  case class DoubleFromHeaderValue(charset: Charset = Charset.defaultCharset) extends FromHeaderValue[Double] {
    val manifest = implicitly[Manifest[Double]]
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

  case class BigDecimalFromHeaderValue(charset: Charset = Charset.defaultCharset) extends FromHeaderValue[BigDecimal] {
    val manifest = implicitly[Manifest[BigDecimal]]
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

  case class LongFromHeaderValue(charset: Charset = Charset.defaultCharset) extends FromHeaderValue[Long] {
    val manifest = implicitly[Manifest[Long]]
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

  case class SeqFromHeaderValue[T](charset: Charset = Charset.defaultCharset)(implicit subConverter: FromHeaderValue[T]) extends FromHeaderValue[Seq[T]] {
    implicit val subManifest = subConverter.manifest
    val manifest = implicitly[Manifest[Seq[T]]]
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

  case class MapFromHeaderValue[T](charset: Charset = Charset.defaultCharset)(implicit subConverter: FromHeaderValue[T]) extends FromHeaderValue[Map[String, T]] {
    implicit val subManifest = subConverter.manifest
    val manifest = implicitly[Manifest[Map[String, T]]]
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

  implicit val defaultStringConversion = StringFromHeaderValue()
  implicit val defaultByteConversion = ByteFromHeaderValue()
  implicit val defaultIntConversion = IntFromHeaderValue()
  implicit val defaultDoubleConversion = DoubleFromHeaderValue()
  implicit val defaultLongConversion = LongFromHeaderValue()
  implicit val defaultFloatConversion = FloatFromHeaderValue()
  implicit val defaultBigDecimalConversion = BigDecimalFromHeaderValue()
  implicit def defaultSeqFromHeaderValue[T](implicit subConverter: FromHeaderValue[T]) = SeqFromHeaderValue[T]()
  implicit def defaultMapFromHeaderValue[T](implicit subConverter: FromHeaderValue[T]) = MapFromHeaderValue[T]()
}
