package com.spingo.op_rabbit

import akka.actor._
import akka.pattern.pipe
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.impl.SourceModule
import akka.stream.{OperationAttributes, SourceShape}
import com.spingo.op_rabbit.subscription.{Subscription, SubscriptionControl, Directive, Directives, RecoveryStrategy, Handler}
import com.thenewmotion.akka.rabbitmq.Channel
import org.reactivestreams.{Publisher, Subscriber}
import scala.annotation.tailrec
import scala.collection.mutable.Queue
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import shapeless._
import shapeless.ops.hlist.Tupler

object RabbitSource {
  type OUTPUT[T] = (Promise[Unit], T)

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
    // If the downstream weak promise is unallocated, and the upstream
    // promise is not yet fulfilled, it's certain that the weak
    // promise cannot fulfill the upstream.
    def lostPromises: Seq[Promise[T]] = {
      val keys = for { (k,v) <- watched.toSeq if v.get.isEmpty } yield {
        watched.remove(k)
        k
      }

      keys.filterNot(_.isCompleted)
    }
  }

  def apply[L <: HList](
    name: String,
    rabbitControl: ActorRef,
    binding: Binding,
    directive: Directive[L],
    qos: Int = 3
  )(implicit ec: ExecutionContext, rabbitErrorLogging: RabbitErrorLogging, refFactory: ActorRefFactory, tupler: Tupler[::[Promise[Unit], L]]) =
    new Publisher[tupler.Out] with SubscriptionControl {

      protected [op_rabbit] val _abortingP = Promise[Unit]
      protected [op_rabbit] val _closingP = Promise[FiniteDuration]
      protected [op_rabbit] val _closedP = Promise[Unit]
      protected [op_rabbit] val _initializedP = Promise[Unit]

      final val closed = _closedP.future
      final val closing: Future[Unit] = _closingP.future.map( _ => () )(ExecutionContext.global)
      final val initialized = _initializedP.future
      final def close(timeout: FiniteDuration = 5 minutes) = _closingP.trySuccess(timeout)
      final def abort() = _abortingP.trySuccess(())

      class RabbitSourceActor extends ActorPublisher[tupler.Out] with ActorLogging {

        import context.dispatcher
        import ActorPublisherMessage.{Cancel, Request}

        // State
        var stopping = false
        var presentQos = qos
        val queue = scala.collection.mutable.Queue.empty[tupler.Out]
        val promiseWatcher = new LostPromiseWatcher[Unit]

        protected case class MessageReceived(promise: Promise[Unit], msg: L)
        protected case class StreamException(e: Throwable)
        protected case class SubscriptionCreated(a: ActorRef)
        protected case object PollLostPromises

        val recoveryStrategy: RecoveryStrategy = new RecoveryStrategy {
          def apply(ex: Throwable, channel: Channel, queueName: String, delivery: Consumer.Delivery): Future[Boolean] = {
            self ! StreamException(ex)
            Future.successful(false)
          }
        }


        val consumerDirective = Directives.consume((binding, rabbitErrorLogging, recoveryStrategy, ec))
        val subscription = new Subscription {
          def config = {
            channel(qos = qos) {
              consumerDirective {
                directive.happly { l =>
                  val p = Promise[Unit]

                  self ! MessageReceived(p, l)

                  ack(p.future)
                }
              }
            }
          }
        }
        // Wire up promises on RabbitSource with Subscription promises.
        // This pattern will likely change.
        _closedP.completeWith(subscription.closed)
        _abortingP.future.foreach(_ => subscription.abort())
        _closingP.future.foreach(subscription.close)
        _initializedP.completeWith(subscription.initialized)

        override def preStart: Unit = {
          rabbitControl ! subscription
          subscription.subscriptionRef foreach ( actorRef => self ! SubscriptionCreated(actorRef) )
          context.system.scheduler.schedule(15 seconds, 15 seconds, self, PollLostPromises)
        }

        var subscriptionActor: Option[ActorRef] = None
        val bufferMax = qos / 2


        def receive = {
          case PollLostPromises =>
            val lost = promiseWatcher.lostPromises.foreach {
              _.failure(new Exception(s"Promise for stream consumer ${name} was garbage collected before it was fulfilled."))
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
            subscription.abort()
            context stop self

          case MessageReceived(promise, msg) =>
            val watchedPromise = promiseWatcher(promise)
            val out = tupler(watchedPromise :: msg)
            queue.enqueue(out)
            drain()
            limitQosOnOverflow()

          case SubscriptionCreated(actorRef) =>
            subscriptionActor = Some(actorRef)
            context watch actorRef

          // preStart adds a hook such that this message is sent as soon as the subscription is closed
          case Terminated(actorRef) if (Some(actorRef) == subscriptionActor) =>
            subscriptionActor = None
            stopping = true
            tryStop()
        }

        private def tryStop(): Unit =
          if (queue.length == 0)
            onCompleteThenStop()


        private def drain(): Unit =
          while ((totalDemand > 0) && (queue.length > 0)) {
            onNext(queue.dequeue())
          }

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

      override def subscribe(sub: Subscriber[_ >: tupler.Out]): Unit = {
        val leActor = refFactory.actorOf(Props(new RabbitSourceActor))
        ActorPublisher[tupler.Out](leActor).subscribe(sub)
      }
    }
}
