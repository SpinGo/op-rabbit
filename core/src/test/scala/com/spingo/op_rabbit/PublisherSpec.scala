package com.spingo.op_rabbit

import com.rabbitmq.client.AMQP.Channel
import org.scalatest.{FunSpec, Matchers}

class PublisherSpec extends FunSpec with Matchers {
  describe("Publisher.exchange") {
    it("receives a Concrete exchange definition, and topic key") {
      val exchange = Exchange.direct("hi")
      val publisher = Publisher.exchange(exchange, "hi")
      publisher.exchangeName shouldBe "hi"
      publisher.routingKey shouldBe "hi"
    }
  }
}
