package com.spingo.op_rabbit

package object stream {

  import scala.concurrent.{Promise, ExecutionContext}

  type AckTup[T] = (Promise[Unit], T)
  case object MessageNacked extends Exception(s"A published message was nacked by the broker.")
}
