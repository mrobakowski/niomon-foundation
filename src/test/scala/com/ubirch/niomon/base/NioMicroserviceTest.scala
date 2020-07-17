package com.ubirch.niomon.base

import com.typesafe.scalalogging.StrictLogging
import com.ubirch.niomon.healthcheck.HealthCheckSuccess
import io.prometheus.client.CollectorRegistry
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.tagobjects.Slow
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, Awaitable}

class NioMicroserviceTest extends FlatSpec with Matchers with EmbeddedKafka with StrictLogging with BeforeAndAfterAll with BeforeAndAfterEach {
  "NioMicroservice" should "work" in {
    withRunningKafka {
      var n = 0
      val microservice = NioMicroserviceLive[String, String]("test", new NioMicroserviceLogic.Simple(_) {
        override def process(record: String): (String, String) = {
          n += 1
          s"foobar$n" -> "default"
        }
      })

      val control = microservice.run

      publishStringMessageToKafka("test-input", "eins")
      publishStringMessageToKafka("test-input", "zwei")
      publishStringMessageToKafka("test-input", "drei")

      val records = consumeNumberStringMessagesFrom("test-output", 3)

      records.size should equal(3)
      records should contain allOf("foobar1", "foobar2", "foobar3")

      val readyStatus = await(microservice.healthCheckServer.ready())
      readyStatus shouldBe a[HealthCheckSuccess]

      await(control.drainAndShutdown()(microservice.system.dispatcher))
    }
  }

  it should "send error to error topic and continue to work if error topic configured" in {
    withRunningKafka {
      // NOTE: look at application.conf in test resources for relevant config
      val microservice = NioMicroserviceLive[String, String]("test-with-error", new NioMicroserviceLogic.Simple(_) {
        var first = true

        override def process(record: String): (String, String) = {
          if (first) {
            first = false
            throw new RuntimeException("foobar")
          } else {
            "barbaz" -> "default"
          }
        }
      })

      val control = microservice.run

      publishStringMessageToKafka("test-with-error-input", "quux")
      publishStringMessageToKafka("test-with-error-input", "kex")

      implicit val d: StringDeserializer = new StringDeserializer
      val records = consumeNumberMessagesFromTopics[String](Set("test-with-error-output", "error"), 2)

      records.size should equal(2)
      records.keys should contain only("test-with-error-output", "error")
      records("test-with-error-output") should contain only "barbaz"
      records("error") should contain only """{"error":"RuntimeException: foobar","causes":[],"microservice":"test-with-error","requestId":null}"""

      await(control.drainAndShutdown()(microservice.system.dispatcher))
    }
  }

  it should "shutdown on error with no error topic configured" in {
    withRunningKafka {
      val microservice = NioMicroserviceLive[String, String]("test1", new NioMicroserviceLogic.Simple(_) {
        override def process(record: String): (String, String) = {
          throw new RuntimeException("foobar")
        }
      })
      val after = microservice.runUntilDone

      publishStringMessageToKafka("test-input", "quux")

      val res = await(after.failed)

      res shouldBe a[RuntimeException]
      res.getMessage should equal("foobar")
    }
  }

  it should "continue processing after kafka goes down for a while" taggedAs(Slow) ignore {
    withRunningKafka {
      val microservice = NioMicroserviceLive[String, String]("faulty-kafka", new NioMicroserviceLogic.Simple(_) {
        override def process(record: String): (String, String) = s"success-$record" -> "default"
      })
      val control = microservice.run

      publishStringMessageToKafka("faulty-kafka-input", "one")
      publishStringMessageToKafka("faulty-kafka-input", "two")
      publishStringMessageToKafka("faulty-kafka-input", "three")

      val records = consumeNumberStringMessagesFrom("faulty-kafka-output", 3)

      records.size should equal(3)
      records should contain allOf("success-one", "success-two", "success-three")

      // emulate kafka going down...
      logger.debug("Shutting down kafka...")
      EmbeddedKafka.stop()
      Thread.sleep(5000)
      val _ = EmbeddedKafka.start()
      logger.debug("Kafka should be back up again")

      publishStringMessageToKafka("faulty-kafka-input", "four")
      publishStringMessageToKafka("faulty-kafka-input", "five")
      publishStringMessageToKafka("faulty-kafka-input", "six")

      val recordsAfterFault = consumeNumberStringMessagesFrom("faulty-kafka-output", 3)

      recordsAfterFault.size should equal(3)
      recordsAfterFault should contain allOf("success-four", "success-five", "success-six")

      await(control.drainAndShutdown()(microservice.system.dispatcher))
    }
  }

  it should "handle long running NioMicroserviceLogics" taggedAs(Slow) ignore {
    val microservice = NioMicroserviceLive[String, String]("test-long", new NioMicroserviceLogic.Simple(_) {
      override def process(record: String): (String, String) = {
        Thread.sleep(20000)
        s"success-$record" -> "default"
      }
    })
    val control = microservice.run

    publishStringMessageToKafka("test-long-input", "one")
    publishStringMessageToKafka("test-long-input", "two")
    publishStringMessageToKafka("test-long-input", "three")

    implicit val d: StringDeserializer = new StringDeserializer()
    val records = consumeNumberMessagesFromTopics[String](Set("test-long-output"), 3, timeout = 1.minute)
      .values.flatten.toList

    records.size should equal(3)
    records should contain allOf("success-one", "success-two", "success-three")

    await(control.drainAndShutdown()(microservice.system.dispatcher))
  }

  def await[T](x: Awaitable[T]): T = Await.result(x, Duration.Inf)

  override def beforeEach(): Unit = {
    CollectorRegistry.defaultRegistry.clear() // workaround for global prometheus state
  }

  // comment this region out to get kafka per test
  // region OneKafka
  override def beforeAll(): Unit = {
    val _ = EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

  def withRunningKafka(body: => Any): Any = body

  // endregion
}
