package com.spingo.op_rabbit
package stream

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Try,Failure}

import akka.actor._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.spingo.op_rabbit.Directives._
import com.spingo.op_rabbit.helpers.RabbitTestHelpers
import com.timcharper.acked.AckedSource
import com.spingo.scoped_fixtures.ScopedFixtures
import org.scalatest.{FunSpec, Matchers}

class MessagePublisherSinkSpec extends FunSpec with ScopedFixtures with Matchers with RabbitTestHelpers {
  implicit val executionContext = ExecutionContext.global

  val _queueName = ScopedFixture[String] { setter =>
    val name = s"test-queue-rabbit-control-${Math.random()}"
    deleteQueue(name)
    val r = setter(name)
    deleteQueue(name)
    r
  }

  trait RabbitFixtures {
    implicit val materializer = ActorMaterializer()
    val exceptionReported = Promise[Boolean]
    implicit val errorReporting = new RabbitErrorLogging {
      def apply(name: String, message: String, exception: Throwable, consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
        exceptionReported.success(true)
      }
    }
    val queueName = _queueName()
    val rabbitControl = actorSystem.actorOf(Props(new RabbitControl))
    val range = (0 to 16)
    val qos = 8
  }

  describe("PublisherSink") {
    it("publishes all messages consumed, and acknowledges the promises") {
      new RabbitFixtures {
        implicit val recoveryStrategy = RecoveryStrategy.limitedRedeliver()

        val (subscription, consumed) = RabbitSource(
          rabbitControl,
          channel(qos),
          consume(queue(queueName, durable = true, exclusive = false, autoDelete = false)),
          body(as[Int])).
          acked.
          take(range.length.toLong).
          toMat(Sink.fold(List.empty[Int]) {
            case (acc, v) =>
              acc ++ List(v)
          })(Keep.both).run

        await(subscription.initialized)

        val data = range map { i => (Promise[Unit], i) }

        val published = AckedSource(data).
          map(Message.queue(_, queueName)).
          to(MessagePublisherSink.acked("test-sink", rabbitControl, actorSystem))

        published.run()
        await(Future.sequence(data.map(_._1.future))) // this asserts that all of the promises were fulfilled
        await(consumed) should be (range)
      }
    }

    it("propagates publish exceptions to promise") {
      new RabbitFixtures {
        val factory = Message.factory(Publisher.queue(Queue.passive("no-existe")))
        val sink = MessagePublisherSink.acked("test-sink", rabbitControl, actorSystem)

        val data = range map { i => (Promise[Unit], i) }

        val published = AckedSource(data).
          map(Message(_, Publisher.queue(Queue.passive("no-existe")))).
          to(sink)

        published.run()
        val Failure(ex) = Try(await(data.head._1.future))
        ex.getMessage should include ("no queue 'no-existe'")
      }
    }
  }
}
