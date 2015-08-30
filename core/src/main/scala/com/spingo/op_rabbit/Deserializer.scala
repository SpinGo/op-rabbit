package com.spingo.op_rabbit

import scala.util.control.NonFatal

trait Deserializer[A, B] extends (A => Deserialized[B]) {}
object Deserializer extends DeserializerLowerPriorityImplicits {

  implicit def fromFunction2Converter[A, B](implicit f: A ⇒ B) =
    new Deserializer[A, B] {
      def apply(a: A) = {
        try Right(f(a))
        catch {
          case NonFatal(ex) ⇒ Left(ParseExtractRejection(ex.toString, ex))
        }
      }
    }

  implicit def liftToTargetOption[A, B](implicit converter: Deserializer[A, B]) =
    new Deserializer[A, Option[B]] {
      def apply(value: A) = converter(value) match {
        case Right(a)              ⇒ Right(Some(a))
        case Left(ex: ValueExpectedExtractRejection) ⇒ Right(None)
        case Left(error)           ⇒ Left(error)
      }
    }
}

private[op_rabbit] abstract class DeserializerLowerPriorityImplicits {
  implicit def lift2SourceOption[A, B](converter: Deserializer[A, B]) = liftToSourceOption(converter)
  implicit def liftToSourceOption[A, B](implicit converter: Deserializer[A, B]) = {
    new Deserializer[Option[A], B] {
      def apply(value: Option[A]) = value match {
        case Some(a) ⇒ converter(a)
        case None    ⇒ Left(ValueExpectedExtractRejection(s"Expected a value when extracting the contents of an Option"))
      }
    }
  }
}
