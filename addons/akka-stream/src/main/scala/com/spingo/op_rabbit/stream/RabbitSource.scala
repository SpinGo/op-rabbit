package com.spingo.op_rabbit
package stream

import akka.actor._
import akka.pattern.pipe
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.scaladsl.Source
import akka.stream.{Graph, Materializer}
import com.thenewmotion.akka.rabbitmq.Channel
import com.timcharper.acked.AckedSource
import org.reactivestreams.Publisher
import scala.annotation.tailrec
import scala.collection.mutable.Queue
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import shapeless._

private [op_rabbit] case class StreamException(e: Throwable)

trait MessageExtractor[Out] {
  def unapply(m: Any): Option[(Promise[Unit], Out)]
}

class RabbitSourceActor[T](
  abort: Promise[Unit],
  consumerStopped: Future[Unit],
  initialQos: Int,
  MessageReceived: MessageExtractor[T] ) extends ActorPublisher[(Promise[Unit], T)] with ActorLogging {

  type Out = (Promise[Unit], T)
  import ActorPublisherMessage.{Cancel, Request}

  // State
  var stopping = false
  var presentQos = initialQos
  val queue = scala.collection.mutable.Queue.empty[Out]

  override def preStart: Unit = {
    implicit val ec = SameThreadExecutionContext
    consumerStopped.foreach { _ => self ! Status.Success }
  }

  var subscriptionActor: Option[ActorRef] = None
  val bufferMax = initialQos / 2


  def receive = {
    case Request(demand) =>
      drain()
      if (stopping) tryStop()

    // A stream consumer detached
    case Cancel =>
      context stop self


    // sent by the StreamRabbitConsumer if there is a deserialization error or other issue
    case StreamException(ex) =>
      onError(ex)
      abort.success(())
      context stop self

    case MessageReceived(promise, msg) =>
      queue.enqueue((promise, msg))
      drain()
      limitQosOnOverflow()

    case Status.Success =>
      subscriptionActor = None
      stopping = true
      tryStop()
  }

  private def tryStop(): Unit =
    if (queue.length == 0)
      onCompleteThenStop()


  private def drain(): Unit =
    while ((totalDemand > 0) && (queue.length > 0))
      onNext(queue.dequeue())

  private def limitQosOnOverflow(): Unit = {
    subscriptionActor.foreach { ref =>
      // TODO - think this through
      val desiredQos = if(queue.length > bufferMax) 1 else presentQos
      if (desiredQos == presentQos) subscriptionActor.foreach { ref =>
        ref ! Subscription.SetQos(desiredQos)
        presentQos = desiredQos
      }
    }
  }
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
  )(implicit refFactory: ActorRefFactory, tupler: HListToValueOrTuple[L], errorReporting: RabbitErrorLogging, recoveryStrategy: RecoveryStrategy) = {
    type Out = (Promise[Unit], tupler.Out)
    case class MessageReceived(promise: Promise[Unit], msg: tupler.Out)

    val messageReceivedExtractor = new MessageExtractor[tupler.Out] {
      type AcceptedType = MessageReceived
      def unapply(m: Any) = m match {
        case d: MessageReceived =>
          Some((d.promise, d.msg))
        case _ =>
          None
      }
    }

    val abort = Promise[Unit]
    val consumerStopped = Promise[Unit]
    val leActor: ActorRef = refFactory.actorOf(Props(new RabbitSourceActor(abort, consumerStopped.future, channelDirective.config.qos, messageReceivedExtractor )))

    def interceptingRecoveryStrategy = new RecoveryStrategy {
      def apply(ex: Throwable, channel: Channel, queueName: String, as: ActorSystem): Handler = { (p, delivery) =>
        recoveryStrategy(ex, channel, queueName, as)(p, delivery)
        // if recovery strategy fails, then yield the exception through the stream
        p.future.onFailure({ case ex => leActor ! StreamException(ex) })(SameThreadExecutionContext)
      }
    }

    val subscription = Subscription.run(rabbitControl) {
      import Directives._
      channelDirective {
        bindingDirective({
          handler.happly { l =>
            val p = Promise[Unit]

            leActor ! MessageReceived(p, tupler(l))

            ack(p.future)
          }
        })(errorReporting, interceptingRecoveryStrategy, SameThreadExecutionContext)
      }
    }

    abort.future.foreach({ _ => subscription.abort() })(SameThreadExecutionContext)
    consumerStopped.completeWith(subscription.closed)
    new AckedSource(Source(ActorPublisher[Out](leActor)).mapMaterializedValue(_ => subscription))
  }
}
