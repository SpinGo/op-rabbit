package com.spingo.op_rabbit
package stream

import akka.actor.ActorRef
import akka.pattern.ask
import akka.stream.scaladsl.{Flow, Keep, Sink}
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import com.timcharper.acked.AckedSink

/**
  ConfirmedPublisherSink is an [[https://github.com/timcharper/acked-stream/blob/master/src/main/scala/com/timcharper/acked/AckedSink.scala AckedSink]] which publishes each message it receives, as configured by the provided messageFactory. After it receives confirmation from RabbitMQ that the published message was received, it acknowledges the element received from the AckedSource.

  Using a [[RabbitSource$ RabbitSource]] with a ConfirmedPublisherSink is a great way to get persistent, recoverable streams.
  */
object ConfirmedPublisherSink {
  /**
    @param rabbitControl An actor
    @param timeoutAfter The duration for which we'll wait for a message to be acked; note, timeouts and non-acknowledged messages will cause the Sink to throw an exception.
    */
  def apply[T](rabbitControl: ActorRef, messageFactory: MessageForPublicationLike.Factory[T, ConfirmedMessage], timeoutAfter: FiniteDuration = 30 seconds, qos: Int = 8): AckedSink[T, Future[Unit]] = AckedSink {
    implicit val akkaTimeout = akka.util.Timeout(timeoutAfter)
    Flow[(Promise[Unit], T)].
      map { case (p, payload) =>
        val msg = messageFactory(payload)

        implicit val ec = SameThreadExecutionContext
        val acked = (rabbitControl ? msg).mapTo[Boolean] flatMap { a =>
          if (a)
            Future.successful(())
          else
            Future.failed(MessageNacked)
        }
        p.completeWith(acked)
        acked
      }.
      mapAsync(qos)(identity). // resolving the futures in the stream causes back-pressure in the case of a rabbitMQ connection being unavailable; specifying a number greater than 1 is for buffering
      toMat(Sink.ignore)(Keep.right)
  }
}
