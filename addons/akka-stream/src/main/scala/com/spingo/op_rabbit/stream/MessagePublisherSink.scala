package com.spingo.op_rabbit
package stream

import akka.actor.{ActorRef,Props}
import akka.actor.FSM
import akka.pattern.ask
import akka.stream.scaladsl.Sink
import akka.stream.actor._
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import com.timcharper.acked.AckedSink
import scala.util.{Try,Success,Failure}

private [stream] object MessagePublisherSinkActor {
  sealed trait State
  case object Running extends State
  case object Stopping extends State
  case object AllDoneFuturePlease
}

private class MessagePublisherSinkActor(rabbitControl: ActorRef, timeoutAfter: FiniteDuration, qos: Int) extends ActorSubscriber with FSM[MessagePublisherSinkActor.State, Unit] {
  import ActorSubscriberMessage._
  import MessagePublisherSinkActor._

  private val queue = scala.collection.mutable.Map.empty[Long, Promise[Unit]]
  private val completed = Promise[Unit]

  startWith(Running, ())

  override val requestStrategy = new MaxInFlightRequestStrategy(max = qos) {
    override def inFlightInternally: Int = queue.size
  }

  override def postRestart(reason: Throwable): Unit = {
    stopWith(Failure(reason))
    super.postRestart(reason)
  }

  private def stopWith(reason: Try[Unit]): Unit = {
    context stop self
    completed.tryComplete(reason)
  }

  when(Running) {
    case Event(response: Message.ConfirmResponse, _) =>
      handleResponse(response)
      stay

    case Event(OnError(e), _) =>
      completed.tryFailure(e)
      goto(Stopping)

    case Event(OnComplete, _) =>
      goto(Stopping)
  }

  when(Stopping) {
    case Event(response: Message.ConfirmResponse, _) =>
      handleResponse(response)
      if(queue.isEmpty)
        stop
      else
        stay
  }

  whenUnhandled {
    case Event(OnNext((p: Promise[Unit] @unchecked, msg: Message)), _) =>
      queue(msg.id) = p
      rabbitControl ! msg
      stay

    case Event(MessagePublisherSinkActor.AllDoneFuturePlease,_) =>
      sender ! completed.future
      stay
  }

  onTransition {
    case Running -> Stopping if queue.isEmpty  =>
      stopWith(Success(()))
  }

  onTermination {
    case e: StopEvent =>
      stopWith(Success(()))
  }

  private val handleResponse: Message.ConfirmResponse => Unit = {
    case Message.Ack(id) =>
      queue.remove(id).get.success(())

    case Message.Nack(id) =>
      queue.remove(id).get.failure(new MessageNacked(id))

    case Message.Fail(id, exception: Throwable) =>
      queue.remove(id).get.failure(exception)
  }
}

/**
  A MessagePublisherSink (an [[https://github.com/timcharper/acked-stream/blob/master/src/main/scala/com/timcharper/acked/AckedSink.scala AckedSink]]) publishes each input [[Message]], and either acks or fails the upstream element, depending on [[Message$.ConfirmResponse ConfirmResponse]].

  Using a [[RabbitSource$ RabbitSource]] with a [[MessagePublisherSink$ MessagePublisherSink]] is a great way to get persistent, recoverable streams.

  Note - MessagePublisherSink uses ActorPublisher and due to AkkaStream limitations, it DOES NOT abide your configured supervisor strategy.

  == [[com.spingo.op_rabbit.Message$.ConfirmResponse Message.ConfirmResponse]] handling ==

  After the sink publishes the [[Message]], it listens for the [[Message$.ConfirmResponse Message.ConfirmResponse]], and handles it accordingly:

  - On [[Message$.Ack Message.Ack]], ack the upstream element.

  - On [[Message$.Nack Message.Nack]], fail the upstream element with
    [[MessageNacked]]. '''Does not''' throw a stream
    exception. Processing continues.

  - On [[Message$.Fail Message.Fail]], fail the upstream element with
    publisher exception. '''Does not''' throw a stream
    exception. Processing continues.

  == Future[Unit] materialized type: ==

  This sinks materialized type is Future[Unit]. The following applies:

  - It yields any upstream failure as soon as it reaches the sink (potentially before messages are confirmed).
  - After the stream completes, and all [[Message$.ConfirmResponse Message.ConfirmResponse]]'s have have been processed, the Future[Unit] is completed.
  */
object MessagePublisherSink {
  /**
    @param rabbitControl An actor
    @param timeoutAfter The duration for which we'll wait for a message to be acked; note, timeouts and non-acknowledged messages will cause the upstream elements to fail. The sink will not throw an exception.
    */
  def apply(rabbitControl: ActorRef, timeoutAfter: FiniteDuration = 30.seconds, qos: Int = 8): AckedSink[Message, Future[Unit]] = AckedSink {
    Sink.actorSubscriber[(Promise[Unit], Message)](Props(new MessagePublisherSinkActor(rabbitControl, timeoutAfter, qos))).
      mapMaterializedValue { subscriber =>
        implicit val akkaTimeout = akka.util.Timeout(timeoutAfter)
        implicit val ec = SameThreadExecutionContext

        (subscriber ? MessagePublisherSinkActor.AllDoneFuturePlease).mapTo[Future[Unit]].flatMap(identity)
      }
  }
}
