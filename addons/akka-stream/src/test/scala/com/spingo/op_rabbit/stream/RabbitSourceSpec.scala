package com.spingo.op_rabbit
package stream

import akka.actor._
import akka.stream.{ ActorAttributes, ActorMaterializer, ActorMaterializerSettings, Supervision }
import akka.stream.scaladsl.{Keep, Sink}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.spingo.op_rabbit.Directives._
import com.spingo.op_rabbit.helpers.{DeleteQueue, RabbitTestHelpers}
import com.spingo.scoped_fixtures.ScopedFixtures
import com.timcharper.acked.{ AckedSink, AckedSource }
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.Try

class RabbitSourceSpec extends FunSpec with ScopedFixtures with Matchers with RabbitTestHelpers {

  val queueName = ScopedFixture[String] { setter =>
    val name = s"test-queue-rabbit-control-${Math.random()}"
    deleteQueue(name)
    val r = setter(name)
    deleteQueue(name)
    r
  }

  trait RabbitFixtures {
    implicit val executionContext = ExecutionContext.global
    implicit val materializer = ActorMaterializer()
    val exceptionReported = Promise[Boolean]
    implicit val errorReporting = new RabbitErrorLogging {
      def apply(name: String, message: String, exception: Throwable, consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
        exceptionReported.trySuccess(true)
      }
    }
    val range = (0 to 16).toList
    val qos = 8

    implicit val recoveryStrategy = RecoveryStrategy.none
    lazy val binding = queue(queueName(), durable = true, exclusive = false, autoDelete = false)
    lazy val source = RabbitSource(
      rabbitControl,
      channel(qos = qos),
      consume(binding),
      body(as[Int]))
  }

  describe("streaming from rabbitMq") {
    it("subscribes to a stream of events and shuts down sanely") {
      new RabbitFixtures {
        lazy val promises = range map (_ => Promise[Unit])
        val (subscription, result) = source.
          map { i => promises(i).success(()); i }.
          acked.
          toMat(Sink.fold(0)(_ + _))(Keep.both).run

        await(subscription.initialized)
        (range) foreach { i => rabbitControl ! Message.queue(i, queueName()) }
        await(Future.sequence(promises.map(_.future)))
        // test is done; let's stop the stream
        subscription.close()

        await(result) should be (136)
        await(subscription.closed) // assert that subscription gets closed
      }
    }

    it("yields serialization exceptions through the stream and shuts down the subscription") {
      new RabbitFixtures {

        val (subscription, result) = source.runAckMat(Keep.both)

        await(subscription.initialized)
        (range) foreach { i => rabbitControl ! Message.queue("a", queueName()) }
        // test is done; let's stop the stream

        a [java.lang.NumberFormatException] should be thrownBy {
          await(result)
        }

        a [java.lang.NumberFormatException] should be thrownBy {
          await(subscription.closed) // assert that subscription gets closed
        }

        await(exceptionReported.future) should be (true)
      }
    }

    it("recovers from connection interruptions by replaying unacked messages") {
      new RabbitFixtures {
        try {
          var delivered = range map { _ => Promise[Unit] }
          var floodgate = Promise[Unit]

          val (subscription, result) = source.
            mapAsync(qos) { i =>
              delivered(i).trySuccess(())
              floodgate.future map { _ => i }
            }.
            runAckMat(Keep.both)

          await(subscription.initialized)
          (range) foreach { i => rabbitControl ! Message.queue(i, queueName()) }
          delivered.take(8).foreach(p => await(p.future))
          await(Future.sequence( delivered.take(8).map(_.future)))
          // we should get up to 8 delivered

          delivered = range map { _ => Promise[Unit] }

          reconnect(rabbitControl)

          // open the floodgate; let's wait for the entire stream to complete
          floodgate.success(())
          delivered.foreach(p => await(p.future)) // if this fails, it means it did not replay every message

          // test is done; let's stop the stream
          subscription.close()
          await(subscription.closed)
        } finally {
          println("deleting")
          val delete = DeleteQueue(queueName())
          rabbitControl ! DeleteQueue(queueName())
          Try {
            await(delete.processed, 1.second)
            println("deleted")
          }
        }
      }
    }

    it("doesn't stop the subscription if the stream resumes and recovery strategy is defined") {
      new RabbitFixtures {
        override val qos = 1
        override implicit val recoveryStrategy = RecoveryStrategy.nack(requeue = false)

        val (subscription, result) = source.take(range.length.toLong).
          map { n =>
            if (n % 2 == 0)
              throw new RuntimeException("no evens please")
            else
              n
          }.
          acked.
          toMat(Sink.fold(List.empty[Int])(_ :+ _))(Keep.both).
          withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider)).
          run

        await(subscription.initialized)
        range foreach { i => rabbitControl ! Message.queue(i, queueName()) }
        await(result).shouldBe(range.filter(_ % 2 == 1).toList)
        await(subscription.closed)

        // Prove that the recovery strategy was applied (no elements were re-queued)
        val (subscription2, result2) = source.take(1).
          acked.
          toMat(Sink.head)(Keep.both).
          run

        await(subscription2.initialized)
        rabbitControl ! Message.queue(99, queueName())

        await(subscription2.closed)
        await(result2) shouldBe 99
      }
    }

    it("processes the recovery strategy even when the stream crashes") {
      new RabbitFixtures {
        override implicit val recoveryStrategy: RecoveryStrategy = RecoveryStrategy { (queueName, channel, exception) =>
          Thread.sleep(1000) // make this recoveryStrategy take extraordinaly long
          RecoveryStrategy.abandonedQueue()(queueName, channel, exception)
        }

        var count = 0
        val abandonsSource = RabbitSource(
          rabbitControl,
          channel(qos = 1),
          consume(pqueue(s"op-rabbit.abandoned.${queueName()}")),
          body(as[Int]))
        val (subscription, result) = source.
          map { _ =>
            count = count + 1
            throw new RuntimeException("boogie")
          }.
          runAckMat(Keep.both)
        await(subscription.initialized)

        range foreach { i => rabbitControl ! Message.queue(i, queueName()) }

        a [RuntimeException] shouldBe thrownBy {
          await(result)
        }

        await(subscription.closed)
        count shouldBe 1

        // Make sure the element was published to the abandon queue
        await(abandonsSource.runWith(AckedSink.head)) shouldBe (0)

        // Make sure the first element was acked
        await(source.runWith(AckedSink.head)) shouldBe (1)
      }
    }


    it("ends the stream gracefully when the subscription is closed") {
      (0 to 1) foreach { time =>
        new RabbitFixtures {
          override lazy val binding = queue(queueName(), durable = false, exclusive = false, autoDelete = true)
          val promises = range map { i => Promise[Unit]}
          val (subscription, result) = source.
            map(promises(_).success(())).
            runAckMat(Keep.both)
          await(subscription.initialized)
          (range) foreach { i => rabbitControl ! Message.queue(i, queueName()) }
          promises.foreach(p => await(p.future))
          subscription.close()
          await(result)
        }
      }
    }
  }

}
