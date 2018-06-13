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

  private val UriPramPath = "uri"
  private val HostsPramPath = "hosts"
  private val PortPramPath = "port"
  private val UsernamePramPath = "username"
  private val PasswordPramPath = "password"
  private val VirtualHostPramPath = "virtual-host"
  private val SslPramPath = "ssl"
  private val ConnectionTimeoutParamPath = "connection-timeout"
  private val ConnectionTimeoutParamName = "connection_timeout"
  private val HeartbeatParamName = "heartbeat"
  private val ChannelMaxParamName = "channel_max"
  private val AuthMechanismParamName = "auth_mechanism"
  sealed case class UriQueryParams(
                                    connectionTimeout: Int = DefaultConnectionTimeout,
                                    heartbeat: Int = ConnectionFactory.DEFAULT_HEARTBEAT,
                                    channelMax: Int = ConnectionFactory.DEFAULT_CHANNEL_MAX,
                                    authMechanism: DefaultSaslConfig = DefaultSaslConfig.PLAIN
                                  )

  def fromConfig(config: Config = RabbitConfig.connectionConfig): ConnectionParams = {
    if (config.hasPath(UriPramPath)) fromUri(new URI(config.getString(UriPramPath))) else fromParameters(config)
  }

  @deprecated(message = "The parameters configuration is deprecated if favor of the URL configuration", since = "2.1.0")
  private def fromParameters(config: Config): ConnectionParams = {
    val hosts = readHosts(config).toArray
    val port = config.getInt(PortPramPath)

    ConnectionParams(
      hosts = hosts.map { h => new Address(h, port) },
      connectionTimeout = config.getDuration(ConnectionTimeoutParamPath, java.util.concurrent.TimeUnit.MILLISECONDS).toInt,
      username = config.getString(UsernamePramPath),
      password = config.getString(PasswordPramPath),
      virtualHost = config.getString(VirtualHostPramPath),
      ssl = config.getBoolean(SslPramPath)
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
    val default = (ConnectionFactory.DEFAULT_USER, ConnectionFactory.DEFAULT_PASS)
    auth.fold(default) { candidate =>
      candidate.split(":").toList match {
        case usr :: psswd :: _ => (usr, psswd)
        case _ => default
      }
    }
  }

  private def readHosts(config: Config): Seq[String] = {
    Try(config.getStringList(HostsPramPath).asScala).getOrElse(readCommaSeparatedHosts(config))
  }

  private def readCommaSeparatedHosts(config: Config): Seq[String] =
    config
      .getString(HostsPramPath)
      .split(",")
      .map(_.trim)

  private def parseAndValidateUriQuery(query: Option[String]): UriQueryParams = {
    query.fold(Array.empty[String])(_.split('&')).foldLeft(UriQueryParams()) {
      case (resp, par) =>
        val index = par.indexOf('=')
        val key = par.substring(0, index)
        val value = par.substring(index + 1)
        key match {
          case ConnectionTimeoutParamName =>
            Try(resp.copy(connectionTimeout = value.toInt)).recover {
              case _: NumberFormatException => throw new IllegalArgumentException(s"The URL parameter [$ConnectionTimeoutParamName] value should be integer")
            }.get
          case HeartbeatParamName =>
            Try(resp.copy(heartbeat = value.toInt)).recover {
              case _: NumberFormatException => throw new IllegalArgumentException(s"The URL parameter [$HeartbeatParamName] value should be integer")
            }.get
          case ChannelMaxParamName =>
            Try(resp.copy(channelMax = value.toInt)).recover {
              case _: NumberFormatException => throw new IllegalArgumentException(s"The URL parameter [$ChannelMaxParamName] value should be integer")
            }.get
          case AuthMechanismParamName =>
            resp.copy(authMechanism = string2DefaultSaslConfig(value))
          case _ =>
            throw new IllegalArgumentException(s"The URL parameter [$key] is not supported")
        }
    }
  }

  private def string2DefaultSaslConfig(value: String): DefaultSaslConfig = {
    value.toLowerCase() match {
      case "external" => DefaultSaslConfig.EXTERNAL
      case "plain" => DefaultSaslConfig.PLAIN
      case _ => throw new IllegalArgumentException(s"The URL parameter [$AuthMechanismParamName] supports PLAIN or EXTERNAL values only")
    }
  }
}
