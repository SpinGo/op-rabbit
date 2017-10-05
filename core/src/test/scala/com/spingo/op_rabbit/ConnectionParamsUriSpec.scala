package com.spingo.op_rabbit

import com.rabbitmq.client.Address
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConversions._

class ConnectionParamsSpec extends FunSpec with Matchers {
  private val defaultConfig = ConfigFactory.load()
  private val connectionPath = "op-rabbit.connection"
  private val hostsPath = "hosts"

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
        params.connectionTimeout should equal(10000)
        params.ssl shouldBe true
        params.virtualHost should equal("vhost")
      }
    }
  }

  private def makeConfig(hosts: String, baseConfig: Config): Config =
    defaultConfig.withValue(hostsPath, ConfigValueFactory.fromAnyRef(hosts))

  private def makeConfig(hosts: List[String], baseConfig: Config): Config =
    defaultConfig.withValue(hostsPath, ConfigValueFactory.fromAnyRef(hosts))

  describe("fromConfig constructor") {
    describe("reading from artificially created config") {
      it("accepts hosts as array of strings") {
        val hosts = List("localhost", "github.com")
        val config = makeConfig(hosts, defaultConfig)

        assertHosts(config, hosts)
      }

      it("accepts hosts as comma-separated string") {
        val hosts = List("localhost", "github.com")
        val config = makeConfig(hosts.mkString(","), defaultConfig)

        assertHosts(config, hosts)
      }

      it("trims extra whitespace in comma-separated list") {
        val hosts = List("localhost", "github.com")
        val config = makeConfig(hosts.map(str => " " + str + " ").mkString(","), defaultConfig)

        assertHosts(config, hosts)
      }
    }

    describe("reading from real config file") {
      val expectedHosts = List("localhost", "127.0.0.1")
      it("accepts hosts as array of strings") {
        val config = ConfigFactory.load("config_fixtures/hosts.list.conf").getConfig("op-rabbit.connection")

        assertHosts(config, expectedHosts)
      }

      it("accepts hosts as comma-separated string") {
        val config = ConfigFactory.load("config_fixtures/hosts.comma_separated.conf").getConfig("op-rabbit.connection")

        assertHosts(config, expectedHosts)
      }
    }
  }

  private def assertHosts(config: Config, expectedHosts: List[String]) = {
    val connectionParams = ConnectionParams.fromConfig(config)
    val expectedPort = config.getInt("port")
    connectionParams.hosts should contain theSameElementsAs expectedHosts.map(host => new Address(host, expectedPort))
  }
}
