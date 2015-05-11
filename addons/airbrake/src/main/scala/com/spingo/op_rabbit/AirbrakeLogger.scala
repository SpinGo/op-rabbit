package com.spingo.op_rabbit

import airbrake.{AirbrakeNoticeBuilder, AirbrakeNotifier}
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.AMQP.BasicProperties
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._

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
  lazy val fromConfig = {
    val airbrakeConfig = ConfigFactory.load().getConfig("airbrake")
    val (appName, key, environment) = (airbrakeConfig.getString("app-name"), airbrakeConfig.getString("key"), airbrakeConfig.getString("environment"))
    new AirbrakeLogger(appName, key, environment)
  }
}
