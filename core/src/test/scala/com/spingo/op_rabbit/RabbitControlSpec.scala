package com.spingo.op_rabbit

import akka.actor._
import akka.pattern.ask
import com.spingo.scoped_fixtures.ScopedFixtures
import helpers.RabbitTestHelpers
import org.scalatest.{Inside, FunSpec, Matchers}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._

class RabbitControlSpec extends FunSpec with ScopedFixtures with Matchers with RabbitTestHelpers with Inside {

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
        implicit val recoveryStrategy = RecoveryStrategy.limitedRedeliver()
        var count = 0
        val promises = (0 to 2).map { i => Promise[Int] }.toList

        val subscription = Subscription.run(rabbitControl) {
          import Directives._
          channel(qos = 5) {
            consume(queue(queueName, durable = false, exclusive = false)) {
              body(as[Int]) { i =>
                println(s"received $i")
                count += 1
                promises(i).success(i)
                ack
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

  describe("Message publication") {
    it("responds with Ack(msg.id) on delivery confirmation") {
      implicit val recoveryStrategy = RecoveryStrategy.limitedRedeliver()
      new RabbitFixtures {
        val subscription = Subscription.run(rabbitControl) {
          import Directives._
          channel(qos = 5) {
            consume(queue(queueName, durable = false, exclusive = false)) {
              ack
            }
          }
        }
        await(subscription.initialized)

        val msg = Message(5, Publisher.queue(queueName))
        await(rabbitControl ? msg) should be (Message.Ack(msg.id))
        deleteQueue(queueName)
      }
    }

    it("handles connection interruption without dropping messages") {
      implicit val recoveryStrategy = RecoveryStrategy.limitedRedeliver()
      new RabbitFixtures {
        val doneConfirm = Promise[Set[Int]]
        val doneReceive = Promise[Set[Int]]

        val counter = actorSystem.actorOf(Props(new Actor {
          var received = Set.empty[Int]
          var confirmed = Set.empty[Int]
          def receive = {
            case ('confirm, n: Int) =>
              println(s"Counter: confirm $n")
              confirmed = confirmed + n
              if (n == -1)
                doneConfirm.trySuccess(confirmed)
            case ('receive, n: Int) =>
              println(s"Counter: receive $n")
              received = received + n
              if (n == -1)
                doneReceive.trySuccess(received)
          }
        }))

        val subscription = Subscription.run(rabbitControl) {
          import Directives._
          channel(qos = 1) {
            consume(queue(queueName, durable = true, exclusive = false)) {
              body(as[Int]) { i =>
                println(s"Consumer received $i")
                counter ! (('receive, i))
                ack
              }
            }
          }
        }
        await(subscription.initialized)

        val sender = actorSystem.actorOf(Props(new Actor {
          val factory = Message.factory(Publisher.queue(queueName))
          var i = 0
          override def preStart(): Unit = {
            context.system.scheduler.schedule(0.seconds, 5.millis, self, 'beat)
          }

          def receive = {
            case 'beat if i == 40 =>
              doSend(-1)
              context.stop(self)
            case 'beat =>
              i = i + 1
              if (i == 20)
                Future { reconnect(rabbitControl) }
              doSend(i)
          }

          def doSend(n: Int): Unit = {
            (rabbitControl ? factory(n)) foreach { case m: Message.Ack =>
              counter ! (('confirm, n))
            }
          }
        }))

        val received = await(doneReceive.future)
        val confirmed = await(doneConfirm.future)
        received shouldBe confirmed

        deleteQueue(queueName)
      }
    }

    it("fails delivery to non-existent queues when using VerifiedQueuePublisher") {
      new RabbitFixtures {
        val msg = Message(1, Publisher.queue(Queue.passive("non-existent-queue")))
        val response = await((rabbitControl ? msg).mapTo[Message.Fail])

        response.id should be (msg.id)
        response.exception.getMessage() should include ("no queue 'non-existent-queue'")
      }
    }

    it("overcomes channel-closes resulting in routing messages to non-existent exchanges") {
      implicit val recoveryStrategy = RecoveryStrategy.limitedRedeliver()
      new RabbitFixtures {

        Subscription.run(rabbitControl) {
          import Directives._
          channel() {
            consume(Binding.direct(Queue("somegoodqueue"), Exchange.direct("somegoodexchange"), List("somegoodroutingkey"))) {
              body(as[Int]) { n =>
                ack
              }
            }
          }
        }

        inside(await(rabbitControl ? Message.exchange("bad", "somebadexchange", "somebadroutingkey"))) {
          case Message.Fail(_, ex: com.rabbitmq.client.ShutdownSignalException) =>
            ex.getReason.protocolMethodId shouldBe 40
        }

        for(i <- 1 to 10) {
          val msg = Message.exchange(i, "somegoodexchange", "somegoodroutingkey")
          inside(await(rabbitControl ? msg)) {
            case Message.Ack(id) =>
              msg.id shouldBe id
          }
        }
      }
    }

  }
}
