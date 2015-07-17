package com.spingo.op_rabbit.consumer

import akka.actor._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.ActorMaterializerSettings
import akka.stream.Materializer
import akka.stream.Supervision
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.{Sink, Source}
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Envelope
import com.spingo.op_rabbit.QueueMessage
import com.spingo.op_rabbit.consumer.Directives._
import com.spingo.op_rabbit.helpers.{DeleteQueue, RabbitTestHelpers}
import com.spingo.scoped_fixtures.ScopedFixtures
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Try,Success,Failure}
import com.spingo.op_rabbit.SameThreadExecutionContext

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
    val range = (0 to 16) toList
    val qos = 8

    implicit val recoveryStrategy = RecoveryStrategy.none
    lazy val binding = queue(queueName(), durable = true, exclusive = false, autoDelete = false)
    lazy val source = RabbitSource(
      "very-stream",
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
          map { i => promises(i).success(); i }.
          acked.
          toMat(Sink.fold(0)(_ + _))(Keep.both).run

        await(subscription.initialized)
        (range) foreach { i => rabbitControl ! QueueMessage(i, queueName()) }
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
        (range) foreach { i => rabbitControl ! QueueMessage("a", queueName()) }
        // test is done; let's stop the stream

        a [java.lang.NumberFormatException] should be thrownBy {
          await(result)
        }

        await(subscription.closed) // assert that subscription gets closed
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
          (range) foreach { i => rabbitControl ! QueueMessage(i, queueName()) }
          delivered.take(8).foreach(p => await(p.future))
          await(Future.sequence( delivered.take(8).map(_.future)))
          // we should get up to 8 delivered

          delivered = range map { _ => Promise[Unit] }

          reconnect(rabbitControl)

          // open the floodgate; let's wait for the entire stream to complete
          floodgate.success()
          delivered.foreach(p => await(p.future)) // if this fails, it means it did not replay every message

          // test is done; let's stop the stream
          subscription.close()
          await(subscription.closed)
        } finally {
          println("deleting")
          val delete = DeleteQueue(queueName())
          rabbitControl ! DeleteQueue(queueName())
          Try {
            await(delete.processed, 1 second)
            println("deleted")
          }
        }
      }
    }

    it("handles being brought down, and started up again") {
      (0 to 1) foreach { time =>
        new RabbitFixtures {
          override lazy val binding = queue(queueName(), durable = false, exclusive = false, autoDelete = true)
          val promises = range map { i => Promise[Unit]}
          val (subscription, result) = source.
            map(promises(_).success(())).
            runAckMat(Keep.both)
          await(subscription.initialized)
          (range) foreach { i => rabbitControl ! QueueMessage(i, queueName()) }
          promises.foreach(p => await(p.future))
          subscription.close()
          await(result)
        }
      }
    }
  }

  describe("RabbitSource operations") {
    def runLeTest[T, U](input: scala.collection.immutable.Iterable[T] = Range(1, 20))(fn: RabbitSource[T, Unit] => Future[U])(implicit materializer: Materializer) = {
      val withPromise = (Stream.continually(Promise[Unit]) zip input).toList
      val promises = withPromise.map(_._1)
      implicit val ec = ExecutionContext.Implicits.global

      val results = (promises zip Range(0, Int.MaxValue)).map { case (p, i) =>
        p.future.map { r => Success(r) }.recover { case e => Failure(e) }
      }

      val returnValue = await(fn(new RabbitSource(Source(withPromise))))
      (results.map { f => Try { await(f, duration = 100 milliseconds) } toOption }, returnValue)
    }

    def asOptBool(s: Seq[Option[Try[Unit]]]) =
      s.map { case Some(Success(_)) => Some(true); case Some(Failure(_)) => Some(false); case _ => None }

    def assertAcked(completions: Seq[Option[Try[Unit]]]) =
      asOptBool(completions) should be (List.fill(completions.length)(Some(true)))

    def assertOperationCatches(fn: (Throwable, RabbitSource[Int, Unit]) => RabbitSource[_, Unit]) = {
      case object LeException extends Exception("le fail")
      implicit val materializer = ActorMaterializer(ActorMaterializerSettings(actorSystem).withSupervisionStrategy(Supervision.resumingDecider : Supervision.Decider))
      val (completions, result) = runLeTest(Range.inclusive(1,20)) { s => fn(LeException, s).runAck }
      completions should be (List.fill(completions.length)(Some(Failure(LeException))))
    }

    describe("filter") {
      it("acks the promises that fail the filter") {
        implicit val materializer = ActorMaterializer()
        val (completions, result) = runLeTest(Range.inclusive(1,20)) { _.
          filter { n => n % 2 == 0 }.
          acked.
          runWith(Sink.fold(0)(_ + _))
        }
        result should be (110)
        implicit val ec = SameThreadExecutionContext
        assertAcked(completions)
      }

      it("catches exceptions and propagates them to the promise") {
        assertOperationCatches { (e, source) => source.filter { n => throw e } }
      }
    }

    describe("map") {
      it("catches exceptions and propagates them to the promise") {
        assertOperationCatches { (e, source) => source.map { n => throw e } }
      }
    }

    describe("grouped") {
      it("acks all messages when the group is acked") {
        implicit val materializer = ActorMaterializer()
        val (completions, result) = runLeTest(Range.inclusive(1,20)) { _.
          grouped(20).
          acked.
          runWith(Sink.fold(0) { (a, b) => a + 1 })
        }
        result should be (1)
        assertAcked(completions)
      }

      it("rejects all messages when the group fails") {
        assertOperationCatches { (e, source) => source.grouped(5).map( n => throw e) }
      }
    }

    describe("mapAsync") {
      it("catches exceptions and propagates them to the promise") {
        assertOperationCatches { (e, source) => source.mapAsync(4) { n => throw e } }
        assertOperationCatches { (e, source) => source.mapAsync(4) { n => Future.failed(e) } }
      }
    }

    describe("mapAsyncUnordered") {
      it("catches exceptions and propagates them to the promise") {
        assertOperationCatches { (e, source) => source.mapAsync(4) { n => throw e } }
        assertOperationCatches { (e, source) => source.mapAsync(4) { n => Future.failed(e) } }
      }
    }

    describe("groupBy") {
      it("catches exceptions and propagates them to the promise") {
        assertOperationCatches { (e, source) => new RabbitSource(source.groupBy { n => throw e })}
      }
    }

    describe("conflate") {
      it("catches exceptions and propagates them to the promise") {
        assertOperationCatches { (e, source) => source.conflate(n => throw e ){ (a: Int, b) => 5 }}
      }
    }
    describe("log") {
      // TODO - it looks like log does not resume exceptions! Bug in akka-stream?
      // it("catches exceptions and propagates them to the promise") {
      //   assertOperationCatches { (e, source) => source.log("hi", { n => throw e}) }
      // }
    }

    describe("mapConcat") {
      it("Acks messages that are filtered by returning List.empty") {
        implicit val materializer = ActorMaterializer()
        val (completions, result) = runLeTest(Range.inclusive(1,20)) { _.
          mapConcat ( n => List.empty[Int] ).
          acked.
          runWith(Sink.fold(0)(_ + _))
        }
        result should be (0)
        assertAcked(completions)
      }

      it("Acks messages that are split into multiple messages") {
        implicit val materializer = ActorMaterializer()
        val (completions, result) = runLeTest(Range.inclusive(1,20)) { _.
          mapConcat ( n => List(n, n) ).
          acked.
          runWith(Sink.fold(0)(_ + _))
        }
        result should be (420)
        assertAcked(completions)
      }

      it("catches exceptions and propagates them to the promise") {
        assertOperationCatches { (e, source) => source.mapConcat { n => throw e } }
      }
    }
  }
}
