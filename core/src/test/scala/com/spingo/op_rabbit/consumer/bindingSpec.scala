package com.spingo.op_rabbit.consumer

import akka.actor._
import akka.pattern.ask
import com.spingo.op_rabbit.{ConfirmedMessage, ExchangePublisher}
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

  describe("HeadersBinding") {
    it("properly declares the header binding with appropriate type matching") {
      import scala.concurrent.ExecutionContext.Implicits.global

      val queueName = _queueName()
      val stringReceived = Promise[String]
      val intReceived = Promise[String]
      val subscriptionInt = new Subscription {
        def config = channel() {
          consume(HeadersBinding(
            queueName = queueName + "int",
            "test-headers-exchange",
            headers = List(Header("thing", 1)),
            autoDelete = true,
            durable = false)) {
            body(as[String]) { a =>
              intReceived.success(a)
              ack
            }
          }
        }
      }

      val subscriptionString = new Subscription {
        def config = channel() {
          consume(HeadersBinding(
            queueName = queueName + "string",
            "test-headers-exchange",
            headers = List(Header("thing", "1")),
            autoDelete = true,
            durable = false)) {
            body(as[String]) { a =>
              println(s"String consumer has string $a")
              stringReceived.success(a)
              ack
            }
          }
        }
      }

      rabbitControl ! subscriptionString
      rabbitControl ! subscriptionInt

      await(subscriptionInt.initialized)
      await(subscriptionString.initialized)

      rabbitControl ! ConfirmedMessage(ExchangePublisher("test-headers-exchange"), "string", List(Header("thing", "1")))
      rabbitControl ! ConfirmedMessage(ExchangePublisher("test-headers-exchange"), "int", List(Header("thing", 1)))

      await(stringReceived.future) should be ("string")
      await(intReceived.future) should be ("int")
    }
  }
}
