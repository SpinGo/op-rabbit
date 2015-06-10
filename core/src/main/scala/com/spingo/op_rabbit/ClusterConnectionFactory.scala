package com.spingo.op_rabbit

import com.rabbitmq.client.ConnectionFactory

/**
  Used internally by RabbitMQ ConnectionFactory doesn't presently provide a way to configure multiple hosts; since akka-rabbitmq just uses ConnectionFactory.newConnection().
  */
class ClusterConnectionFactory extends ConnectionFactory() {
  import com.rabbitmq.client.{Address, Connection}
  var hosts = Array.empty[Address]
  /**
    Configures connection factory to connect to one of the following hosts.
    */
  def setHosts(newHosts: Array[Address]): Unit = { hosts = newHosts }

  override def getHost = {
    if (hosts.nonEmpty)
      s"{${hosts.mkString(",")}}"
    else
      super.getHost
  }

  override def newConnection(): Connection = {
    if (hosts.nonEmpty)
      this.newConnection(hosts)
    else
      super.newConnection()
  }
}

