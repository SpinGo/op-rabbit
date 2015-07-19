package com.spingo.op_rabbit.stream

import akka.actor._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.spingo.op_rabbit.{ConfirmedMessage, QueuePublisher, RabbitControl}
import com.spingo.op_rabbit.consumer.{HListToValueOrTuple, RabbitErrorLogging}
import com.spingo.op_rabbit.consumer.Directives._
import com.spingo.op_rabbit.helpers.RabbitTestHelpers
import com.spingo.scoped_fixtures.ScopedFixtures
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import shapeless._

class AckedSinkSpec extends FunSpec with ScopedFixtures with Matchers with RabbitTestHelpers {

  describe("fold") {
    it("acknowledges promises as they are folded in") {
      case class LeException(msg: String) extends Exception(msg)
      val input = (Stream.continually(Promise[Unit]) zip Range.inclusive(1, 5)).toList
      implicit val materializer = ActorMaterializer()
      Try(await(AckedSource(input).runWith(AckedSink.fold(0){ (reduce, e) =>
        if(e == 4) throw LeException("dies here")
        e + reduce
      })))
      input.map { case (p, _) =>
        p.tryFailure(LeException("didn't complete"))
        Try(await(p.future)) match {
          case Success(_) => None
          case Failure(LeException(msg)) => Some(msg)
        }
      } should be (Seq(None, None, None, Some("dies here"), Some("didn't complete")))
    }
  }
}
