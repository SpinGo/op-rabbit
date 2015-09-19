package com.spingo.op_rabbit.binding

import com.rabbitmq.client.Channel

// TODO - rename to AnyConcreteness
sealed trait Concreteness
sealed trait Concrete extends Concreteness {}
sealed trait Abstract extends Concreteness {}

sealed trait TopologyDefinition {
  def declare(channel: Channel): Unit
}

trait QueueDefinition[+T <: Concreteness] extends TopologyDefinition {
  /**
    The name of the message queue this binding declares.
    */
  val queueName: String
}

trait ExchangeDefinition[T <: Concreteness] extends TopologyDefinition {
  val exchangeName: String
}
trait Binding extends TopologyDefinition with QueueDefinition[Abstract] with ExchangeDefinition[Abstract]
