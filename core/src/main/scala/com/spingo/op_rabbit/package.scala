package com.spingo

import scala.concurrent.{Promise,Future}
import shapeless._

package object op_rabbit {
  type Handler = (Promise[ReceiveResult], Delivery) => Unit
  type Directive1[T] = Directive[::[T, HNil]]
  type Deserialized[T] = Either[Rejection.ExtractRejection, T]

  protected val futureUnit: Future[Unit] = Future.successful(())
}
