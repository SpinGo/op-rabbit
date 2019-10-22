package com.spingo.op_rabbit
package stream

import akka.actor._
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import com.timcharper.acked.{AckedSource, AckTup}
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.{Failure,Success}
import shapeless._
import scala.concurrent.duration._

private [op_rabbit] case class StreamException(e: Throwable)

private [op_rabbit] trait MessageExtractor[Out] {
  def unapply(m: Any): Option[(Promise[Unit], Out)]
}

/**
  * Creates an op-rabbit consumer whose messages are delivered through an
  * [[https://github.com/timcharper/acked-stream/blob/master/src/main/scala/com/timcharper/acked/AckedSource.scala
  * AckedSource]]. Message order guarantees are maintained.
  *
  * IMPORTANT NOTE ON ACKNOWLEDGED MESSAGES
  *
  * (if you are seeing unacknowledged messages accumulated, followed by the
  * stream progress halting, you will want to pay special attention to this)
  *
  * The 'Acked' variety of streams provide type-safe guarantees that
  * acknowledgments aren't dropped on the floor. Filtering a message from the
  * stream (via collect, filter, or mapConcat -> Nil), for example, will cause
  * the incomming RabbitMQ message to be acknowledged.
  *
  * If you begin constructing your own Acked components and interacting with the
  * `AckTup[T]` type directly (or, `(Promise[Unit], T)`), you must take caution
  * that the promise is not dropped on the floor. This means, every time an
  * exception could be thrown, you must catch it and propagate said exception to
  * the Promise by calling `promise.fail(ex)`.
  *
  * If you integrate with a stream compoment that does not support acknowledged
  * streams, you will probably want to acknowledge the message before sending
  * messages to it (by using the `.acked` acked-stream operation). If it is
  * important that you acknowledge messages after the flow is complete, and the
  * library doesn't provide a reliable way to propagate element exceptions, you
  * will likely want exception to crash the stream (IE: don't resume the stream
  * on exception). Otherwise, if using a resuming decider, you risk elements
  * being dropped and unacknowledged messages accumulating.
  *
  * Example:
  *
  * {{{
  * import com.spingo.op_rabbit.Directives._
  * RabbitSource(
  *   rabbitControl,
  *   channel(qos = 3),
  *   consume(queue("such-queue", durable = true, exclusive = false, autoDelete = false)),
  *   body(as[Person])).
  *   runForeach { person =>
  *     greet(person)
  *   } // after each successful iteration the message is acknowledged.
  * }}}
  */
object RabbitSource {
  /**
    * @param rabbitControl Op-rabbit management ActorRef
    * @param channelDirective An unbound [[http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html#com.spingo.op_rabbit.Directives@channel(qos:Int):com.spingo.op_rabbit.ChannelDirective ChannelDirective]]
    * @param bindingDirective An unbound [[http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html#com.spingo.op_rabbit.Directives@consume(binding:com.spingo.op_rabbit.Binding):com.spingo.op_rabbit.BindingDirective BindingDirective]]; informs the stream from which queue it should pull messages.
    * @param handler A [[http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html#com.spingo.op_rabbit.Directive Directive]], either simple or compound, describing how the elements passed into the stream are to be formed.
    */
  def apply[L <: HList](
    rabbitControl: ActorRef,
    channelDirective: ChannelDirective,
    bindingDirective: BindingDirective,
    handler: Directive[L]
  )(implicit tupler: HListToValueOrTuple[L], errorReporting: RabbitErrorLogging, recoveryStrategy: RecoveryStrategy, ec: ExecutionContext) = {
    val src = Source.queue[AckTup[tupler.Out]](channelDirective.config.qos, OverflowStrategy.backpressure).mapMaterializedValue { input =>
      val subscription = Subscription.run(rabbitControl) {
        import Directives._
        channelDirective {
          bindingDirective({
            handler.happly { l =>
              val p = Promise[Unit]
              input.offer((p, tupler(l)))
              ack(p.future)
            }
          })(errorReporting, recoveryStrategy, SameThreadExecutionContext)
        }
      }

      subscription.closed.onComplete {
        case Failure(ex) =>
          if (! input.watchCompletion.isCompleted)
            input.fail(ex)
        case Success(_) =>
          if (! input.watchCompletion.isCompleted)
            input.complete()
      }

      // Down stream terminated?
      input.watchCompletion.onComplete {
        case Success(_) =>
          /* If the consumer abort is scheduled before the ReceiveResult.Fail message (representing the failure
           * potentially causing the stream to stop), then we enter a potential infinite loop in which the message
           * causing failure is infinitely requeued.
           *
           * A brief delay makes this unlikely race result near impossible, since RecoveryStrategy is processes
           * synchronously and the channel will not close until after the consumer actor dies.
           *
           * A better solution would be if AckedStreams yielded a stream of acknowledgements, rather than using
           * promises, so we could wait for the stream of promises to end.
           */
          subscription.close(500.millis)
        case Failure(ex) =>
          subscription.close(500.millis)
      }

      subscription
    }

    new AckedSource(src)
  }
}
