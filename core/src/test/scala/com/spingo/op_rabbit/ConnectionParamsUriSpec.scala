package com.spingo.op_rabbit

import com.rabbitmq.client.Address
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConversions._

class ConnectionParamsUriSpec extends FunSpec with Matchers {
  private val defaultConfig = ConfigFactory.load()
  private val connectionPath = "op-rabbit.connection"

  describe("fromConfig constructor") {
    describe("original properties connection configuration") {
      it("compose configuration parameters") {
        val params = ConnectionParams.fromConfig(defaultConfig.getConfig(connectionPath))

        params.hosts should contain theSameElementsAs Seq(new Address("localhost", 5672))
        params.username should equal("guest")
        params.password should equal("guest")
        params.connectionTimeout should equal(1000)
        params.ssl shouldBe false
      }
    }

    describe("URI configuration parameters") {
      it("compose single host configuration") {
        val config = defaultConfig.withValue(connectionPath, ConfigValueFactory.fromMap(Map("uri" -> "amqp://user:secret@localhost:5672/vhost")))
        val params = ConnectionParams.fromConfig(config.getConfig(connectionPath))

        params.hosts should contain theSameElementsAs Seq(new Address("localhost", 5672))
        params.username should equal("user")
        params.password should equal("secret")
        params.connectionTimeout should equal(10000)
        params.ssl shouldBe false
        params.virtualHost should equal("vhost")
      }

      it("compose multiple host configuration") {
        val config = defaultConfig.withValue(connectionPath, ConfigValueFactory.fromMap(Map("uri" -> "amqp://user:secret@localhost:5672,127.0.0.1:5672/vhost")))
        val params = ConnectionParams.fromConfig(config.getConfig(connectionPath))

        params.hosts should contain theSameElementsAs Seq(new Address("localhost", 5672), new Address("127.0.0.1", 5672))
        params.username should equal("user")
        params.password should equal("secret")
        params.connectionTimeout should equal(10000)
        params.ssl shouldBe false
        params.virtualHost should equal("vhost")
      }

      it("compose multiple host configuration with TLS protection") {
        val config = defaultConfig.withValue(connectionPath, ConfigValueFactory.fromMap(Map("uri" -> "amqps://user:secret@localhost:5672,127.0.0.1:5672/vhost")))
        val params = ConnectionParams.fromConfig(config.getConfig(connectionPath))

        params.hosts should contain theSameElementsAs Seq(new Address("localhost", 5672), new Address("127.0.0.1", 5672))
        params.username should equal("user")
        params.password should equal("secret")
        params.ssl shouldBe true
        params.virtualHost should equal("vhost")
        params.connectionTimeout should equal(10000)
        params.requestedHeartbeat should equal(60)
        params.requestedChannelMax should equal(0)
      }

      it("compose multiple host configuration with TLS protection and additional URL parameters") {
        val config = defaultConfig.withValue(connectionPath, ConfigValueFactory.fromMap(Map("uri" -> "amqps://user:secret@localhost:5672,127.0.0.1:5672/vhost?connection_timeout=20000&heartbeat=30&channel_max=2&auth_mechanism=external")))
        val params = ConnectionParams.fromConfig(config.getConfig(connectionPath))

        params.hosts should contain theSameElementsAs Seq(new Address("localhost", 5672), new Address("127.0.0.1", 5672))
        params.username should equal("user")
        params.password should equal("secret")
        params.ssl shouldBe true
        params.virtualHost should equal("vhost")
        params.connectionTimeout should equal(20000)
        params.requestedHeartbeat should equal(30)
        params.requestedChannelMax should equal(2)
        params.saslConfig.getSaslMechanism(Array("EXTERNAL")).getName should equal("EXTERNAL")
      }
    }
  }
}
