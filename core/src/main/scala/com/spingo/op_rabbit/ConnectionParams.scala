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
import scala.collection.JavaConverters._
import scala.util.Try

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
  connectionTimeout: Int = ConnectionParams.DefaultConnectionTimeout,
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
    factory.setClientProperties(clientProperties.asJava)
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
  private val DefaultConnectionTimeout = 10000 /* 10 seconds */

  private val UriParamPath = "uri"
  private val HostsParamPath = "hosts"
  private val PortParamPath = "port"
  private val UsernameParamPath = "username"
  private val PasswordParamPath = "password"
  private val VirtualHostParamPath = "virtual-host"
  private val SslParamPath = "ssl"
  private val ConnectionTimeoutParamPath = "connection-timeout"
  private val ConnectionTimeoutParamName = "connection_timeout"
  private val HeartbeatParamName = "heartbeat"
  private val ChannelMaxParamName = "channel_max"
  private val AuthMechanismParamName = "auth_mechanism"
  sealed case class UriQueryParams(connectionTimeout: Int = DefaultConnectionTimeout,
                                   heartbeat: Int = ConnectionFactory.DEFAULT_HEARTBEAT,
                                   channelMax: Int = ConnectionFactory.DEFAULT_CHANNEL_MAX,
                                   authMechanism: DefaultSaslConfig = DefaultSaslConfig.PLAIN)

  def fromConfig(config: Config = RabbitConfig.connectionConfig): ConnectionParams = {
    if (config.hasPath(UriParamPath)) fromUri(new URI(config.getString(UriParamPath))) else fromParameters(config)
  }

  @deprecated(message = "The parameters configuration is deprecated if favor of the URL configuration", since = "2.1.1")
  private def fromParameters(config: Config): ConnectionParams = {
    val hosts = readHosts(config).toArray
    val port = config.getInt(PortParamPath)

    ConnectionParams(
      hosts = hosts.map { h => new Address(h, port) },
      connectionTimeout = config.getDuration(ConnectionTimeoutParamPath, java.util.concurrent.TimeUnit.MILLISECONDS).toInt,
      username = config.getString(UsernameParamPath),
      password = config.getString(PasswordParamPath),
      virtualHost = config.getString(VirtualHostParamPath),
      ssl = config.getBoolean(SslParamPath)
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
    val params = parseAndValidateUriQuery(Option(uri.getQuery))
    ConnectionParams(
      hosts = hosts,
      username = username,
      password = password,
      virtualHost = Option(uri.getPath).fold(ConnectionFactory.DEFAULT_VHOST)(_.substring(1)),
      ssl = TlsProtectedScheme == uri.getScheme,
      connectionTimeout = params.connectionTimeout,
      requestedHeartbeat = params.heartbeat,
      requestedChannelMax = params.channelMax,
      saslConfig = params.authMechanism
    )
  }

  private def parseUriHosts(hosts: String): Seq[Address] = {
    hosts.split(",").map(Address.parseAddress)
  }

  private def parseUserPassword(auth: Option[String]): (String, String) = {
    auth.fold(ConnectionFactory.DEFAULT_USER -> ConnectionFactory.DEFAULT_PASS) { candidate =>
      candidate.split(":").toList match {
        case usr :: psswd :: _ => (usr, psswd)
        case _ => throw new IllegalArgumentException(s"The URL authority should contains user and password")
      }
    }
  }

  private def readHosts(config: Config): Seq[String] = {
    Try(config.getStringList(HostsParamPath).asScala.toSeq).getOrElse(readCommaSeparatedHosts(config))
  }

  private def readCommaSeparatedHosts(config: Config): Seq[String] =
    config
      .getString(HostsParamPath)
      .split(",")
      .map(_.trim)

  private def parseAndValidateUriQuery(query: Option[String]): UriQueryParams = {
    query.fold(Array.empty[String])(_.split('&')).foldLeft(UriQueryParams()) {
      case (resp, par) =>
        val Array(key, value) = par.split('=')
        key match {
          case ConnectionTimeoutParamName =>
            resp.copy(connectionTimeout = uriParamValue2Int(ConnectionTimeoutParamName, value))
          case HeartbeatParamName =>
            resp.copy(heartbeat = uriParamValue2Int(HeartbeatParamName, value))
          case ChannelMaxParamName =>
            resp.copy(channelMax = uriParamValue2Int(ChannelMaxParamName, value))
          case AuthMechanismParamName =>
            resp.copy(authMechanism = uriParam2DefaultSaslConfig(AuthMechanismParamName, value))
          case _ =>
            throw new IllegalArgumentException(s"The URL parameter [$key] is not supported")
        }
    }
  }

  private def uriParam2DefaultSaslConfig(name: String, value: String): DefaultSaslConfig = {
    value.toLowerCase() match {
      case "external" => DefaultSaslConfig.EXTERNAL
      case "plain" => DefaultSaslConfig.PLAIN
      case _ => throw new IllegalArgumentException(s"The URL parameter [$name] supports PLAIN or EXTERNAL values only")
    }
  }

  private def uriParamValue2Int(name: String, value: String): Int = {
    try {
      value.toInt
    } catch {
      case _: NumberFormatException => throw new IllegalArgumentException(s"The URL parameter [$name] value should be integer")
    }
  }
}
