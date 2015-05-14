package com.spingo.op_rabbit

import akka.actor._
import akka.pattern.pipe
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import com.thenewmotion.akka.rabbitmq.Channel
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import scala.annotation.tailrec
import scala.collection.mutable.Queue
import scala.concurrent.ExecutionContext
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

object RabbitSource {
  type OUTPUT[T] = (Promise[Unit], T)

  // def apply[T](
  //   actorRefFactory: ActorRefFactory,
  //   rabbitControl: ActorRef,
  //   binding: Binding,
  //   name: String,
  //   qos: Int = 3)(implicit
  //     unmarshaller: RabbitUnmarshaller[T],
  //     rabbitErrorLogging: RabbitErrorLogging): RabbitSource[T] = {

  //   val ref = actorRefFactory.actorOf(Props(new RabbitSourceActor[T](
  //     rabbitControl = rabbitControl,
  //     binding = binding,
  //     name = name,
  //     qos = qos)))

  //   new RabbitSource[T](ref)
  // }

  private [op_rabbit] class LostPromiseWatcher[T]() {
    // the key is a strong reference to the upstream promise; the weak-key is the representative promise.
    private var watched = scala.collection.concurrent.TrieMap.empty[Promise[T], scala.ref.WeakReference[Promise[T]]]

    def apply(p: Promise[T])(implicit ec : ExecutionContext): Promise[T] = {
      val weakPromise = Promise[T]
      watched(p) = scala.ref.WeakReference(weakPromise)
      p.completeWith(weakPromise.future) // this will not cause p to have a strong reference to weakPromise
      p.future.onComplete { _ => watched.remove(p) }
      weakPromise
    }

    // polls for lost promises
    def lostPromises: Seq[Promise[T]] = {
      for ((k,v) <- watched.toSeq if v.get.isEmpty) yield {
        watched.remove(k)
        k
      }
    }
  }
}

case class RabbitSource[T](
  name: String,
  qos: Int = 3)(implicit
    unmarshaller: RabbitUnmarshaller[T],
    rabbitErrorLogging: RabbitErrorLogging) extends Consumer with Publisher[RabbitSource.OUTPUT[T]] {

  type O = RabbitSource.OUTPUT[T]
  private val subscriber = Promise[Subscriber[_ >: O]]

  def props(queueName: String) =
    Props(new RabbitSourceActor[T](
      queueName     = queueName,
      name          = name,
      qos           = qos,
      subscriber    = subscriber.future))

  override def subscribe(sub: Subscriber[_ >: O]): Unit =
    subscriber.success(sub)
}

protected class RabbitSourceActor[T](
  queueName: String,
  name: String,
  qos: Int = 3,
  subscriber: Future[Subscriber[_ >: RabbitSource.OUTPUT[T]]])(implicit
    unmarshaller: RabbitUnmarshaller[T],
    rabbitErrorLogging: RabbitErrorLogging) extends ActorPublisher[RabbitSource.OUTPUT[T]] with ActorLogging {

  import RabbitSource._
  import context.dispatcher
  import ActorPublisherMessage.{Cancel, Request}

  // State
  var channel: Option[Channel] = None
  var stopping = false
  var presentQos = qos
  val queue = scala.collection.mutable.Queue.empty[OUTPUT[T]]
  val promiseWatcher = new LostPromiseWatcher[Unit]

  protected case class MessageReceived(promise: Promise[Unit], msg: T)
  protected case class StreamException(e: Throwable)
  protected case object PollLostPromises

  // TODO - manage this / monitor this / integrate with subscription
  val consumer = context.actorOf(
    Props {
      new impl.AsyncAckingRabbitConsumer(
        name             = name,
        queueName        = queueName,
        recoveryStrategy = { (ex, channel, envelop, properties, body) =>
          // TODO - I should propagate this error downwards; this is grounds for closing the stream
          self ! StreamException(ex)
          Future.failed(new Exception("Cowardly refusing to retry a message in a stream source provider"))
        },
        onChannel        = { (channel) =>
          channel.basicQos(presentQos)
        },
        handle           = { (msg: T) =>
          val p = Promise[Unit]
          self ! MessageReceived(p, msg)
          p.future
        }
      )
    }
  )
  context.watch(consumer)

  val bufferMax = qos / 2

  override def preStart: Unit = {
    subscriber.foreach { s =>
      ActorPublisher[RabbitSource.OUTPUT[T]](self).subscribe(s)
    }
    context.system.scheduler.schedule(15 seconds, 15 seconds, self, PollLostPromises)
  }

  def receive = {
    case PollLostPromises =>
      val lost = promiseWatcher.lostPromises.foreach {
        _.tryFailure(new Exception(s"Promise for stream consumer ${name} was garbage collected before it was fulfilled."))
      }


    case Request(demand) =>
      drain()
      if (stopping) tryStop()

    // A stream consumer detached
    case Cancel =>
      context stop self


    // sent by the StreamRabbitConsumer if there is a deserialization error or other issue
    case StreamException(ex) =>
      onError(ex)
      context stop self

    case MessageReceived(promise, msg) =>
      queue.enqueue((promiseWatcher(promise), msg))
      drain()
      limitQosOnOverflow()


    case subscribeCommand @ Consumer.Subscribe(_channel) =>
      channel = Some(_channel)
      consumer ! subscribeCommand

    case c: Consumer.ConsumerCommand =>
      consumer ! c

    // preStart adds a hook such that this message is sent as soon as the subscription is closed
    case Terminated(`consumer`) =>
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
    // TODO - think this through
    val desiredQos = if(queue.length > bufferMax) 1 else presentQos
    try if (desiredQos == presentQos) channel.foreach { channel =>
      channel.basicQos(desiredQos)
      presentQos = desiredQos
    } catch {
      case RabbitExceptionMatchers.NonFatalRabbitException(_) =>
        ()
    }
  }
}
