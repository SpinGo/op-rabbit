package com.spingo.op_rabbit.subscription

import com.rabbitmq.client.Envelope
import com.rabbitmq.client.MessageProperties
import com.spingo.op_rabbit.Consumer.Delivery
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._

class DirectivesSpec extends FunSpec with Matchers {
  val dummyEnvelope = new Envelope(1L, false, "kthx", "bai")

  val acked: Result = Right()

  def await[T](f: Future[T], duration: Duration = 5 seconds) =
    Await.result(f, duration)

  import Directives._
  import com.spingo.op_rabbit.properties._

  def resultFor(delivery: Delivery)(handler: Handler) = {
    val handled = Promise[Result]
    handler(handled, delivery)
    await(handled.future)
  }

  describe("property") {
    it("rejects when the property is not defined") {
      val delivery = Delivery("thing", dummyEnvelope, MessageProperties.TEXT_PLAIN, "hi".getBytes)
      resultFor(delivery) {
        property(ReplyTo) { replyTo =>
          ack()
        }
      } should be (Left(ExtractRejection("Property ReplyTo was not provided")))
    }

    it("yields the value when the property is defined") {
      val delivery = Delivery("thing", dummyEnvelope, builderWithProperties(Seq(ReplyTo("place"))).build, "hi".getBytes)
      resultFor(delivery) {
        property(ReplyTo) { replyTo =>
          replyTo should be ("place")
          ack()
        }
      } should be (acked)
    }
  }

  describe("conjuction") {
    it("yields the value for both directives") {
      val delivery = Delivery("thing", dummyEnvelope, builderWithProperties(Seq(ReplyTo("place"))).build, "hi".getBytes)
      resultFor(delivery) {
        (body(as[String]) & property(ReplyTo)) { (body, replyTo) =>
          replyTo should be ("place")
          body should be ("hi")
          ack()
        }
      } should be (acked)
    }
  }

  describe("|") {
    it("recovers from extractor related rejections") {
      val delivery = Delivery("thing", dummyEnvelope, MessageProperties.TEXT_PLAIN, "hi".getBytes)
      resultFor(delivery) {
        ((property(ReplyTo) & property(AppId)) | (provide("default-reply-to") & provide("default-app"))) { (replyTo, appId) =>
          replyTo should be ("default-reply-to")
          appId should be ("default-app")
          ack()
        }
      } should be (acked)
    }
  }

  describe("optionalProperty") {
    it("yields none when the property is not defined") {
      val delivery = Delivery("thing", dummyEnvelope, MessageProperties.TEXT_PLAIN, "hi".getBytes)
      resultFor(delivery) {
        optionalProperty(ReplyTo) { replyTo =>
          replyTo should be (None)
          ack()
        }
      } should be (acked)
    }
  }
}
