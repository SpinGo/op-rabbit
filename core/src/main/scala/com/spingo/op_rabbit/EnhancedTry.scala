package com.spingo.op_rabbit

import scala.util.{Success, Try}

object EnhancedTry {
  implicit class EnhancedTryImpl[T](t: Try[T]) {
    def toEither: Either[Throwable, T] =
      t.transform(success => Success(Right(success)), exception => Success(Left(exception))).get
  }
}
