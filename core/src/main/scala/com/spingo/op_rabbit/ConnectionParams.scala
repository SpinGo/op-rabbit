package com.spingo.op_rabbit

import java.net.URI

import com.rabbitmq.client.Address
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultSaslConfig
import com.rabbitmq.client.ExceptionHandler
import com.rabbitmq.client.SaslConfig
import com.rabbitmq.client.impl.DefaultExceptionHandler
import com.typesafe.config.Config
import javax.net.SocketFactory

import scala.collection.JavaConversions.mapAsJavaMap

/** Because topology recovery strategy configuration is crucial to how op-rabbit works, we don't allow some options to
  * be specified
  *
  * Modeling the allowed options via a case-class allows the compiler to tell the library user which options aren't
  * allowed.
  */
case class ConnectionParams(
  hosts: Seq[Address] = Seq(new Address(ConnectionFactory.DEFAULT_HOST, ConnectionFactory.DEFAULT_AMQP_PORT)),
  username: String = ConnectionFactory.DEFAULT_USER,
  password: String = ConnectionFactory.DEFAULT_PASS,
  virtualHost: String = ConnectionFactory.DEFAULT_VHOST,
  ssl: Boolean = false,
  // Replace the table of client properties that will be sent to the server during subsequent connection startups.
  clientProperties: Map[String, Object] = Map.empty[String, Object],

  /** Warning - setting this to 0 causes problems when connecting a cluster; if the first host is down, then further
    * hosts may not be tried.
    */
  connectionTimeout: Int = 10000 /* 10 seconds */,
  exceptionHandler: ExceptionHandler = new DefaultExceptionHandler(),
  requestedChannelMax: Int = ConnectionFactory.DEFAULT_CHANNEL_MAX,
  requestedFrameMax: Int = ConnectionFactory.DEFAULT_FRAME_MAX,
  requestedHeartbeat: Int = ConnectionFactory.DEFAULT_HEARTBEAT,
  saslConfig: SaslConfig = DefaultSaslConfig.PLAIN,
  sharedExecutor: Option[java.util.concurrent.ExecutorService] = None,
  shutdownTimeout: Int = ConnectionFactory.DEFAULT_SHUTDOWN_TIMEOUT,
  socketFactory: SocketFactory = SocketFactory.getDefault
) {
  // TODO - eliminate ClusterConnectionFactory after switching to use RabbitMQ's topology recovery features.
  protected [op_rabbit] def applyTo(factory: ClusterConnectionFactory): Unit = {
    factory.setHosts(hosts.toArray)
    factory.setUsername(username)
    factory.setPassword(password)
    factory.setVirtualHost(virtualHost)
    // Replace the table of client properties that will be sent to the server during subsequent connection startups.
    factory.setClientProperties(mapAsJavaMap(clientProperties))
    factory.setConnectionTimeout(connectionTimeout)
    factory.setExceptionHandler(exceptionHandler)
    factory.setRequestedChannelMax(requestedChannelMax)
    factory.setRequestedFrameMax(requestedFrameMax)
    factory.setRequestedHeartbeat(requestedHeartbeat)
    factory.setSaslConfig(saslConfig)
    sharedExecutor.foreach(factory.setSharedExecutor)
    factory.setShutdownTimeout(shutdownTimeout)
    factory.setSocketFactory(socketFactory)
    if (ssl) factory.useSslProtocol()
  }
}

object ConnectionParams {
  private val TlsProtectedScheme = "amqps"

  def fromConfig(config: Config = RabbitConfig.connectionConfig): ConnectionParams = {
    if (config.hasPath("uri")) fromUri(new URI(config.getString("uri"))) else fromParameters(config)
  }

  private def fromParameters(config: Config): ConnectionParams = {
    val hosts = config.getStringList("hosts").toArray(new Array[String](0))
    val port = config.getInt("port")
    ConnectionParams(
      hosts = hosts.map { h => new Address(h, port) },
      connectionTimeout = config.getDuration("connection-timeout", java.util.concurrent.TimeUnit.MILLISECONDS).toInt,
      username = config.getString("username"),
      password = config.getString("password"),
      virtualHost = config.getString("virtual-host"),
      ssl = config.getBoolean("ssl")
    )
  }

  private def fromUri(uri: URI): ConnectionParams = {
    val uriAuthority = uri.getAuthority
    val (hosts, (username, password)) = uriAuthority.indexOf('@') match {
      case -1 =>
        parseUriHosts(uriAuthority) -> parseUserPassword(None)
      case idx =>
        parseUriHosts(uriAuthority.substring(idx + 1)) -> parseUserPassword(Some(uriAuthority.substring(0, idx)))
    }
    ConnectionParams(
      hosts = hosts,
      username = username,
      password = password,
      virtualHost = Option(uri.getPath).fold(ConnectionFactory.DEFAULT_VHOST)(_.substring(1)),
      ssl = TlsProtectedScheme == uri.getScheme
    )
  }

  private def parseUriHosts(hosts: String): Seq[Address] = {
    hosts.split(",").map(Address.parseAddress)
  }

  private def parseUserPassword(auth: Option[String]): (String, String) = {
    val default = (ConnectionFactory.DEFAULT_USER, ConnectionFactory.DEFAULT_PASS)
    auth.fold(default) { candidate =>
      candidate.split(":").toList match {
        case usr :: psswd :: _ => (usr, psswd)
        case _ => default
      }
    }
  }
}
