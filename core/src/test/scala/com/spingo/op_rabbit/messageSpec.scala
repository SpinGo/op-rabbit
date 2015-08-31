package com.spingo.op_rabbit

import org.scalatest.{FunSpec, Matchers}
import com.spingo.op_rabbit.properties._

class MessageSpec extends FunSpec with Matchers {
  case class Data(name: String, age: Int)

  // these are stupid tests...
  describe("QueueMessage") {
    it("creates a message for delivery, serializes the data, and applies the provided properties, and defaults to persistent") {
      val msg = Message.queue(
        "very payload",
        queue = "destination.queue",
        properties = List(ReplyTo("respond.here.please")))

      println(msg.properties)
      msg.data should be ("very payload".getBytes)
      msg.publisher.exchangeName should be ("")
      msg.publisher.routingKey should be ("destination.queue")
      msg.properties.getDeliveryMode should be (2)
      msg.properties.getReplyTo should be ("respond.here.please")
    }
  }

  describe("TopicMessage") {
    it("creates a message for delivery, and applies the provided properties, and defaults to persistent") {
      val msg = Message.topic(
        "very payload",
        routingKey = "destination.topic",
        properties = List(ReplyTo("respond.here.please")))

      println(msg.properties)
      msg.properties.getDeliveryMode should be (2)
      msg.properties.getReplyTo should be ("respond.here.please")
      msg.publisher.routingKey should be ("destination.topic")
      msg.publisher.exchangeName should be (RabbitControl.topicExchangeName)
      msg.properties.getReplyTo should be ("respond.here.please")
    }
  }

  describe("ConfirmedMessage") {
    val msg = Message(Publisher.topic("very.route"), "hi", List(ReplyTo("respond.here.please")))
    msg.properties.getDeliveryMode should be (2)
    msg.properties.getReplyTo should be ("respond.here.please")
  }
}
