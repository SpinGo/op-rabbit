package com.spingo.op_rabbit

import scala.concurrent.{Promise,Future}

package object subscription {
  type Result = Either[Rejection, Unit]
  type Handler = (Promise[Result], Consumer.Delivery) => Unit

  protected [op_rabbit] val futureUnit: Future[Unit] = Future.successful(Unit)
}
