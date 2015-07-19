package com.spingo.op_rabbit

package object stream {

  import scala.concurrent.Promise

  type AckTup[T] = (Promise[Unit], T)
}
