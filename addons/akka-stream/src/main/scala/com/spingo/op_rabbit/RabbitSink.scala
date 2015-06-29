package com.spingo.op_rabbit

import akka.actor._
import akka.pattern.{pipe,ask}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.rabbitmq.client.AMQP.BasicProperties
import com.thenewmotion.akka.rabbitmq.Channel
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

object RabbitSink {
  case object MessageNacked extends Exception(s"A published message was nacked by the broker.")
  /**
    @param timeoutAfter The duration for which we'll wait for a message to be acked; note, timeouts and non-acknowledged messages will cause the Sink to throw an exception.
    */
  def apply[T](name: String, rabbitControl: ActorRef, messageFactory: MessageForPublicationLike.Factory[T, ConfirmedMessage], timeoutAfter: FiniteDuration = 30 seconds): Sink[(Promise[Unit], T), Future[Unit]] = {
    implicit val akkaTimeout = akka.util.Timeout(timeoutAfter)
    Flow[(Promise[Unit], T)].
      map { case (p, payload) =>
        val msg = messageFactory(payload)

        (rabbitControl ? msg).mapTo[Boolean]
      }.
      mapAsync(8)(identity). // resolving the futures in the stream causes back-pressure in the case of a rabbitMQ connection being unavailable; specifying a number greater than 1 is for buffering
      toMat(Sink.foreach { acked =>
        if (! acked) throw MessageNacked
      })(Keep.right)
  }
}
