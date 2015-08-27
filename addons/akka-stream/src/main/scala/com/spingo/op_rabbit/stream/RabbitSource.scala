package com.spingo.op_rabbit.stream

import akka.actor._
import akka.pattern.pipe
import akka.stream.{Graph, Materializer}
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.scaladsl.Source
import com.spingo.op_rabbit.consumer.{ChannelDirective, Delivery, Directive, HListToValueOrTuple, RabbitErrorLogging, RecoveryStrategy, Subscription, BindingDirective}
import com.thenewmotion.akka.rabbitmq.Channel
import org.reactivestreams.Publisher
import scala.annotation.tailrec
import scala.collection.mutable.Queue
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import com.timcharper.acked.AckedSource
import shapeless._

private [op_rabbit] case class StreamException(e: Throwable)

trait MessageExtractor[Out] {
  def unapply(m: Any): Option[(Promise[Unit], Out)]
}

class RabbitSourceActor[T](
  name: String,
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

object RabbitSource {
  def apply[L <: HList](
    name: String,
    rabbitControl: ActorRef,
    channelDirective: ChannelDirective,
    subscriptionDirective: BindingDirective,
    directive: Directive[L]
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
    val leActor: ActorRef = refFactory.actorOf(Props(new RabbitSourceActor(name, abort, consumerStopped.future, channelDirective.config.qos, messageReceivedExtractor )))

    def interceptingRecoveryStrategy = new RecoveryStrategy {
      def apply(ex: Throwable, channel: Channel, queueName: String, delivery: Delivery, as: ActorSystem): Future[Boolean] = {
        val downstream = recoveryStrategy(ex, channel, queueName, delivery, as)
        // if recovery strategy fails, then yield the exception through the stream
        downstream.onFailure({ case ex => leActor ! StreamException(ex) })(SameThreadExecutionContext)
        downstream
      }
    }

    val subscription = new Subscription {
      def config = {
        channelDirective {
          subscriptionDirective({
            directive.happly { l =>
              val p = Promise[Unit]

              leActor ! MessageReceived(p, tupler(l))

              ack(p.future)
            }
          })(errorReporting, interceptingRecoveryStrategy, SameThreadExecutionContext)
        }
      }
    }

    rabbitControl ! subscription

    abort.future.foreach({ _ => subscription.abort() })(SameThreadExecutionContext)
    consumerStopped.completeWith(subscription.closed)
    new AckedSource(Source(ActorPublisher[Out](leActor)).mapMaterializedValue(_ => subscription))
  }
}
