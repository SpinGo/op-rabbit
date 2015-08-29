package com.spingo.op_rabbit

import scala.concurrent.ExecutionContext

// WARNING!!! Don't block inside of Runnable (Future) that uses this.
private[op_rabbit] object SameThreadExecutionContext extends ExecutionContext {
  def execute(r: Runnable): Unit =
    r.run()
  override def reportFailure(t: Throwable): Unit =
    throw new IllegalStateException("problem in op_rabbit internal callback", t)
}
