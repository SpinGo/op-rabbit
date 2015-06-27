package com.spingo.op_rabbit.consumer

import airbrake.{AirbrakeNoticeBuilder, AirbrakeNotifier}
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.AMQP.BasicProperties
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._

/**
  Instantiates a new RabbitErrorLogging strategy that reports exceptions, along with message and message headers, to Airbrake.

  == Instantiating from config ==

  You can call the convenience lazy-getter and have airbrake configuration pulled from the application configuration:

  {{{AirbrakeLogger.fromConfig}}}


  It expects the config be specified as follows:

  {{{
  airbrake {
    app-name = "my-awesome-app"
    key = "deadbeefdeadbeefdeadbeefdeadbeef"
    environment = "production"
  }
  }}}

  @param appName The name of your application
  @param airbrakeKey The secret API key to talk to Airbrake. You'll need to go find this.
  @param environment Your deployment environment (e.g. "Production" / "Staging" / "Dev")

  */
class AirbrakeLogger(appName: String, airbrakeKey: String, environment: String) extends RabbitErrorLogging {
  val log = LoggerFactory.getLogger(getClass.getName)
  def apply(name: String, message: String, exception: Throwable, consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = try {
    val notice = new AirbrakeNoticeBuilder(airbrakeKey, exception, environment) {
      setRequest(s"consumer://$appName/${name}", "consume") // it's a faux URL, but reduces weirdness when clicking in airbrake webapp

      val headerProperties: Map[String, String] = Option(properties.getHeaders) map { _.map { case (k,v) => (s"HEADER:$k", v.toString) }.toMap } getOrElse Map.empty

      session(
        Map("consumerTag" -> consumerTag))

      request(
        Map(
          "body"        -> bodyAsString(body, properties),
          "deliveryTag" -> envelope.getDeliveryTag.toString,
          "redeliver"   -> envelope.isRedeliver.toString,
          "exchange"    -> envelope.getExchange,
          "routingKey"  -> envelope.getRoutingKey) ++ headerProperties)

      environment(
        Map(
          "host" -> java.net.InetAddress.getLocalHost.getHostName,
          "consumer" -> name))

      projectRoot(appName)
    }
    new AirbrakeNotifier().notify(notice.newNotice())
  } catch {
    case e: Throwable => log.error("Unable to send airbrake notification for error", e)
  }
}

object AirbrakeLogger {
  /**
    @see [[AirbrakeLogger]]
    */
  lazy val fromConfig = {
    val airbrakeConfig = ConfigFactory.load().getConfig("airbrake")
    val (appName, key, environment) = (airbrakeConfig.getString("app-name"), airbrakeConfig.getString("key"), airbrakeConfig.getString("environment"))
    new AirbrakeLogger(appName, key, environment)
  }
}
