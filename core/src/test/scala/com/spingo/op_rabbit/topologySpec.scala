package com.spingo.op_rabbit

import akka.actor._
import akka.pattern.ask
import com.spingo.op_rabbit.helpers.RabbitTestHelpers
import com.spingo.op_rabbit.properties.Header
import com.spingo.scoped_fixtures.ScopedFixtures
import com.thenewmotion.akka.rabbitmq.RichConnectionActor
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.Promise
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class bindingSpec extends FunSpec with ScopedFixtures with Matchers with RabbitTestHelpers {
  val _queueName = ScopedFixture[String] { setter =>
    val name = s"test-queue-rabbit-control-${Math.random()}"
    deleteQueue(name)
    val r = setter(name)
    deleteQueue(name)
    r
  }

  describe("FanoutBinding") {
    it("properly declares the fanout binding") {
      import scala.concurrent.ExecutionContext.Implicits.global

      val consumerResult = List(Promise[String], Promise[String])

      val subscriptions = (0 to 1) map { case idx =>
        val queueName = _queueName() + idx
        Subscription.run(rabbitControl) {
          import Directives._
          channel() {
            consume(FanoutBinding(
              Queue(queueName, autoDelete = true),
              Exchange.fanout("test-fanout-exchange", autoDelete = true))) {
              body(as[String]) { a =>
                consumerResult(idx).success(a)
                ack
              }
            }
          }
        }
      }

      subscriptions foreach { s =>
        await(s.initialized)
      }

      rabbitControl ! Message("le value", Publisher.exchange("test-fanout-exchange"), List(Header("thing", "1")))

      consumerResult.map(p => await(p.future)) should be (List("le value", "le value"))
    }
  }

  describe("HeadersBinding") {
    it("properly declares the header binding with appropriate type matching") {
      import scala.concurrent.ExecutionContext.Implicits.global

      val queueName = _queueName()
      val stringReceived = Promise[String]
      val intReceived = Promise[String]
      val subscriptionInt = Subscription.run(rabbitControl) {
        import Directives._
        channel() {
          consume(HeadersBinding(
            Queue(
              queueName = queueName + "int",
              autoDelete = true,
              durable = false),
            Exchange.headers("test-headers-exchange", autoDelete = true),
            headers = List(Header("thing", 1))
          )) {
            body(as[String]) { a =>
              intReceived.success(a)
              ack
            }
          }
        }
      }

      val subscriptionString = Subscription.run(rabbitControl) {
        import Directives._
        channel() {
          consume(HeadersBinding(
            Queue(
              queueName = queueName + "string",
              autoDelete = true,
              durable = false),
            Exchange.headers("test-headers-exchange", autoDelete = true),
            headers = List(Header("thing", "1"))
          )) {
            body(as[String]) { a =>
              println(s"String consumer has string $a")
              stringReceived.success(a)
              ack
            }
          }
        }
      }

      await(subscriptionInt.initialized)
      await(subscriptionString.initialized)

      rabbitControl ! Message("string", Publisher.exchange("test-headers-exchange"), List(Header("thing", "1")))
      rabbitControl ! Message("int", Publisher.exchange("test-headers-exchange"), List(Header("thing", 1)))

      await(stringReceived.future) should be ("string")
      await(intReceived.future) should be ("int")
    }
  }

  describe("TopicBinding") {
    it("properly declares the topic binding with appropriate bindings") {
      import scala.concurrent.ExecutionContext.Implicits.global

      val queueName = _queueName()
      val received = Promise[String]
      val subscription = Subscription.run(rabbitControl) {
        import Directives._
        channel() {
          consume(TopicBinding(
            Queue(queueName + "int", autoDelete = true),
            topics = List("*.*.*")
          )) {
            body(as[String]) { a =>
              received.success(a)
              ack
            }
          }
        }
      }

      await(subscription.initialized)

      rabbitControl ! Message("string", Publisher.topic(".."))

      await(received.future) should be ("string")
    }
  }
}
