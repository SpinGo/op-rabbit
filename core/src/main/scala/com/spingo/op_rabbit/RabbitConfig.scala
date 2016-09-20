package com.spingo.op_rabbit

import akka.actor.ActorSystem
import scala.util.Try
import com.typesafe.config.ConfigFactory

object RabbitConfig {
  // TODO - when removing legacy config support, remember to update reference.conf.
  lazy val connectionConfig =
    ConfigFactory.load().getConfig("op-rabbit.connection")

  lazy val systemConfig =
    ConfigFactory.load().getConfig("op-rabbit")

  def channelDispatcher(implicit system: ActorSystem): String =
    system.settings.config.getString("op-rabbit.channel-dispatcher")
}
