package com.spingo.op_rabbit.stream

import com.spingo.op_rabbit._
import com.spingo.op_rabbit.Message._
import akka.stream.stage.GraphStage
import akka.actor.{ActorRef, Props}
import akka.actor.FSM
import akka.pattern.ask
import akka.stream.scaladsl.Sink
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import com.timcharper.acked.AckedSink
import scala.util.{Try, Success,Failure}
import akka.stream._
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.GraphStageWithMaterializedValue
import akka.stream.stage.InHandler
import akka.stream.scaladsl.Flow
import akka.util.Timeout


private class MessagePublisherSink(rabbitControl: ActorRef, timeoutAfter: FiniteDuration, qos: Int) extends GraphStageWithMaterializedValue[SinkShape[(Promise[Unit],Message)], Future[Unit]] {
  val in = Inlet[(Promise[Unit],Message)]("MessagePublisherSink.in")

  val shape = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Unit]) = {
    val completed = Promise[Unit]()

    val logic = new GraphStageLogic(shape) {
      private val queue = scala.collection.mutable.Map.empty[Long, Promise[Unit]]

      // callback to schedule the rabbitControl responses into the stage
      private val futureCallback = getAsyncCallback[Try[Message.ConfirmResponse]]({ 
        case Success(Message.Ack(id)) =>
          queue.remove(id).get.success(())
          pullIfNeeded()

        case Success(Message.Nack(id)) =>
          queue.remove(id).get.failure(new MessageNacked(id))
          pullIfNeeded()

        case Success(Message.Fail(id, exception: Throwable)) =>
          queue.remove(id).get.failure(exception)
          pullIfNeeded()

        case Failure(exception) => 
          // currently fails the stream - maybe better just fail the message - needs additional context
          fail(exception)
      })

      override def preStart(): Unit =  {
        // we must ensure we can acknowledge messages even on stream complete
        setKeepGoing(true)
        pull(in)
      }
      
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val (promise, msg) = grab(in)
          queue(msg.id) = promise

          val eventualResult = rabbitControl.ask(msg)(Timeout(timeoutAfter)).mapTo[ConfirmResponse]

          // TODO: which EC to schedule the callback onto?
          eventualResult.onComplete(futureCallback.invoke)(materializer.executionContext)

          pullIfNeeded()
        }

        override def onUpstreamFinish(): Unit = {
          if (queue.isEmpty) complete()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          fail(ex)
        }
      })

      private def pullIfNeeded(): Unit = {
        if (isClosed(in) && queue.isEmpty) complete()
        else if (queue.size < qos && !hasBeenPulled(in)) tryPull(in)
      }

      private def complete(): Unit = {
        completed.success(())
        completeStage()
      }

      private def fail(ex: Throwable): Unit = {
        completed.failure(ex)
        failStage(ex)
      }
    }

    (logic, completed.future)
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
  def apply(rabbitControl: ActorRef, timeoutAfter: FiniteDuration = 30 seconds, qos: Int = 8): AckedSink[Message, Future[Unit]] = AckedSink {
    new MessagePublisherSink(rabbitControl, timeoutAfter, qos)
  }
}
