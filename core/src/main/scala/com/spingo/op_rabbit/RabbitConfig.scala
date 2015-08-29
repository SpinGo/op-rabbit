package com.spingo.op_rabbit

import scala.util.Try
import com.typesafe.config.ConfigFactory

object RabbitConfig {
  // TODO - when removing legacy config support, remember to update reference.conf.
  lazy val connectionConfig = {
    val c = ConfigFactory.load()
    Try { c.getConfig("op-rabbit.connection") } getOrElse {
      System.err.println("WARNING! Connection configuration 'op-rabbit.connection' is not defined; falling back to 'rabbitmq'. See the README for the new configuration format; this backwards compatibility will be removed, eventually")
      c.getConfig("rabbitmq")
    }
  }

  lazy val systemConfig = {
    val c = ConfigFactory.load()
    Try { c.getConfig("op-rabbit") } getOrElse {
      System.err.println("WARNING! System configuration 'op-rabbit' is not defined; falling back to 'rabbitmq'. See the README for the new configuration format; this backwards compatibility will be removed, eventually")
      c.getConfig("rabbitmq")
    }
  }
}
