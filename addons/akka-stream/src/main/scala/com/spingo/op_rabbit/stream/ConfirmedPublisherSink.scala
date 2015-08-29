package com.spingo.op_rabbit
package stream

import akka.actor.ActorRef
import akka.pattern.ask
import akka.stream.scaladsl.{Flow, Keep, Sink}
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import com.timcharper.acked.AckedSink

object ConfirmedPublisherSink {
  /**
    @param timeoutAfter The duration for which we'll wait for a message to be acked; note, timeouts and non-acknowledged messages will cause the Sink to throw an exception.
    */
  def apply[T](name: String, rabbitControl: ActorRef, messageFactory: MessageForPublicationLike.Factory[T, ConfirmedMessage], timeoutAfter: FiniteDuration = 30 seconds, qos: Int = 8): AckedSink[T, Future[Unit]] = AckedSink {
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
