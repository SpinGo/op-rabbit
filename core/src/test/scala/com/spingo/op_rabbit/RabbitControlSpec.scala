package com.spingo.op_rabbit

import akka.actor._
import akka.pattern.ask
import com.spingo.scoped_fixtures.ScopedFixtures
import com.thenewmotion.akka.rabbitmq.{ChannelActor, RichConnectionActor}
import helpers.RabbitTestHelpers
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Try}

class RabbitControlSpec extends FunSpec with ScopedFixtures with Matchers with RabbitTestHelpers {

  val _queueName = ScopedFixture[String] { setter =>
    val name = s"test-queue-rabbit-control-${Math.random()}"
    try setter(name)
    finally deleteQueue(name)
  }

  trait RabbitFixtures {
    val queueName = _queueName()
    implicit val executionContext = ExecutionContext.global
    val rabbitControl = rabbitControlFixture()
  }

  describe("pausing subscriptions") {
    it("unsubscribes all subscriptions") {
      new RabbitFixtures {
        import RabbitControl._
        var count = 0
        val promises = (0 to 2) map { i => Promise[Int] } toList

        val subscription = Subscription.run(rabbitControl) {
          import Directives._
          channel(qos = 5) {
            consume(queue(queueName, durable = false, exclusive = false)) {
              body(as[Int]) { i =>
                println(s"received $i")
                count += 1
                promises(i).success(i)
                ack()
              }
            }
          }
        }

        await(subscription.initialized)

        rabbitControl ! Message.queue(0, queueName)
        await(promises(0).future)
        count shouldBe 1

        rabbitControl ! Pause
        Thread.sleep(500) // how do we know that it is paused??? :/

        rabbitControl ! Message.queue(1, queueName)
        Thread.sleep(500) // TODO what to use instead of sleep?
        count shouldBe 1 // unsubscribed, no new messages processed

        rabbitControl ! Run

        rabbitControl ! Message.queue(2, queueName)
        await(Future.sequence(Seq(promises(1).future, promises(2).future)))
        count shouldBe 3 // resubscribed, all messages processed

        // clean up rabbit queue
        // val connectionActor = await(rabbitControl ? GetConnectionActor).asInstanceOf[ActorRef]
        // val channel = connectionActor.createChannel(ChannelActor.props())
        deleteQueue(queueName)
      }
    }
  }

  describe("ConfirmedMessage publication") {
    it("fulfills the published promise on delivery confirmation") {
      new RabbitFixtures {
        val subscription = Subscription.run(rabbitControl) {
          import Directives._
          channel(qos = 5) {
            consume(queue(queueName, durable = false, exclusive = false)) {
              ack()
            }
          }
        }
        await(subscription.initialized)

        val msg = Message(Publisher.queue(queueName), 5)
        await(rabbitControl ? msg)
        deleteQueue(queueName)
      }
    }

    // TODO - make this test not suck
    it("handles connection interruption without dropping messages") {
      new RabbitFixtures {
        var received = List.empty[Int]
        var countConfirmed = 0
        var countReceived = 0
        var lastReceived = -1
        val doneConfirm = Promise[Unit]
        val doneReceive = Promise[Unit]

        val counter = actorSystem.actorOf(Props(new Actor {
          def receive = {
            case ('confirm, -1) =>
              doneConfirm.success()
            case ('receive, -1) =>
              doneReceive.success()
            case ('confirm, n: Int) =>
              println(s"== confirm $n")
              countConfirmed += 1
            case ('receive, n: Int) =>
              println(s"receive $n")
              if(n <= lastReceived) // duplicate message
                ()
              else {
                countReceived += 1
                lastReceived = n
              }
          }
        }))

        val subscription = Subscription.run(rabbitControl) {
          import Directives._
          channel(qos = 1) {
            consume(queue(queueName, durable = true, exclusive = false)) {
              body(as[Int]) { i =>
                counter ! ('receive, i)
                ack()
              }
            }
          }
        }
        await(subscription.initialized)

        val factory = Message.factory(Publisher.queue(queueName))

        var keepSending = true
        val lastSentF = Future {
          var i = 0
          while (keepSending) {
            i = i + 1
            val n = i
            val msg = factory(n)
            (rabbitControl ? msg) foreach { _ =>
              counter ! ('confirm, n)
            }
            Thread.sleep(10) // slight delay as to not overwhelm RAM
          }
          i
        }

        Thread.sleep(100)
        reconnect(rabbitControl)
        keepSending = false
        val lastSent = await(lastSentF)
        val confirmMsg = factory(-1)
        (rabbitControl ? confirmMsg) foreach { _ =>
          counter ! ('confirm, -1)
        }
        await(doneReceive.future)
        await(doneConfirm.future)
        countReceived should be (countConfirmed)

        deleteQueue(queueName)
      }
    }

    it("fails delivery to non-existent queues when using VerifiedQueuePublisher") {
      new RabbitFixtures {
        val Failure(ex: com.rabbitmq.client.ShutdownSignalException) = Try(await((rabbitControl ? Message(VerifiedQueuePublisher("non-existent-queue"), 1)).mapTo[Boolean]))

        ex.getMessage() should include ("no queue 'non-existent-queue'")
      }
    }

  }
}
