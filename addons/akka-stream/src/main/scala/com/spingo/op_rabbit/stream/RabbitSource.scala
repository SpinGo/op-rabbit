package com.spingo.op_rabbit
package stream

import akka.actor._
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import com.thenewmotion.akka.rabbitmq.Channel
import com.timcharper.acked.{AckedSource, AckTup}
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.{Failure,Success}
import shapeless._

private [op_rabbit] case class StreamException(e: Throwable)

private [op_rabbit] trait MessageExtractor[Out] {
  def unapply(m: Any): Option[(Promise[Unit], Out)]
}

/**
  Creates an op-rabbit consumer whose messages are delivered through an [[https://github.com/timcharper/acked-stream/blob/master/src/main/scala/com/timcharper/acked/AckedSource.scala AckedSource]].

  Example:

  {{{
  import com.spingo.op_rabbit.Directives._
  RabbitSource(
    rabbitControl,
    channel(qos = 3),
    consume(queue("such-queue", durable = true, exclusive = false, autoDelete = false)),
    body(as[Person])).
    runForeach { person =>
      greet(person)
    } // after each successful iteration the message is acknowledged.
  }}}
  */
object RabbitSource {
  /**
    @param rabbitControl Op-rabbit management ActorRef
    @param channelDirective An unbound [[http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html#com.spingo.op_rabbit.Directives@channel(qos:Int):com.spingo.op_rabbit.ChannelDirective ChannelDirective]]
    @param bindingDirective An unbound [[http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html#com.spingo.op_rabbit.Directives@consume(binding:com.spingo.op_rabbit.Binding):com.spingo.op_rabbit.BindingDirective BindingDirective]]; informs the stream from which queue it should pull messages.
    @param handler A [[http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html#com.spingo.op_rabbit.Directive Directive]], either simple or compound, describing how the elements passed into the stream are to be formed.
    */
  def apply[L <: HList](
    rabbitControl: ActorRef,
    channelDirective: ChannelDirective,
    bindingDirective: BindingDirective,
    handler: Directive[L]
  )(implicit tupler: HListToValueOrTuple[L], errorReporting: RabbitErrorLogging, recoveryStrategy: RecoveryStrategy) = {
    val src = Source.queue[AckTup[tupler.Out]](channelDirective.config.qos, OverflowStrategy.backpressure).mapMaterializedValue { input =>
      def interceptingRecoveryStrategy = new RecoveryStrategy {
        def apply(ex: Throwable, channel: Channel, queueName: String): Handler = { (p, delivery) =>
          recoveryStrategy(ex, channel, queueName)(p, delivery)
          // if recovery strategy fails, then yield the exception through the stream
          input.fail(ex)
        }
      }

      val subscription = Subscription.run(rabbitControl) {
        import Directives._
        channelDirective {
          bindingDirective({
            handler.happly { l =>
              val p = Promise[Unit]
              input.offer((p, tupler(l)))
              ack(p.future)
            }
          })(errorReporting, interceptingRecoveryStrategy, SameThreadExecutionContext)
        }
      }

      subscription.closed.onComplete {
        case Failure(ex) =>
          if (! input.watchCompletion.isCompleted)
            input.fail(ex)
        case Success(_) =>
          if (! input.watchCompletion.isCompleted)
            input.complete()
      }(ExecutionContext.global)

      input.watchCompletion.onComplete { _ =>
        subscription.abort()
      }(ExecutionContext.global)

      subscription
    }

    new AckedSource(src)
  }
}
