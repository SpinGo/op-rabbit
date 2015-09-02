package com.spingo.op_rabbit

import scala.util.Try
import com.typesafe.config.ConfigFactory

object RabbitConfig {
  // TODO - when removing legacy config support, remember to update reference.conf.
  lazy val connectionConfig =
    ConfigFactory.load().getConfig("op-rabbit.connection")

  lazy val systemConfig =
    ConfigFactory.load().getConfig("op-rabbit")
}
