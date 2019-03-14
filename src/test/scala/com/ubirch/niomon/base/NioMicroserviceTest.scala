package com.ubirch.niomon.base

import com.typesafe.scalalogging.StrictLogging
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, Awaitable}

class NioMicroserviceTest extends FlatSpec with Matchers with EmbeddedKafka with StrictLogging with BeforeAndAfterAll {
  "NioMicroservice" should "work" in {
    withRunningKafka {
      var n = 0
      val microservice = new NioMicroservice[String, String]("test") {
        override def process(input: String): (String, String) = {
          n += 1
          s"foobar$n" -> "default"
        }
      }

      val control = microservice.run

      publishStringMessageToKafka("foo", "eins")
      publishStringMessageToKafka("foo", "zwei")
      publishStringMessageToKafka("foo", "drei")

      val records = consumeNumberStringMessagesFrom("bar", 3)

      records.size should equal(3)
      records should contain allOf("foobar1", "foobar2", "foobar3")

      await(control.drainAndShutdown()(microservice.system.dispatcher))
    }
  }

  it should "send error to error topic and continue to work if error topic configured" in {
    withRunningKafka {
      //noinspection TypeAnnotation
      // NOTE: look at application.conf in test resources for relevant config
      val microservice = new NioMicroservice[String, String]("test-with-error") {
        var first = true

        override def process(input: String): (String, String) = {
          if (first) {
            first = false
            throw new RuntimeException("foobar")
          } else {
            "barbaz" -> "default"
          }
        }
      }
      val control = microservice.run

      publishStringMessageToKafka("foo", "quux")
      publishStringMessageToKafka("foo", "kex")

      implicit val d: StringDeserializer = new StringDeserializer
      val records = consumeNumberMessagesFromTopics[String](Set("bar", "error"), 2)

      records.size should equal(2)
      records.keys should contain only ("bar", "error")
      records("bar") should contain only "barbaz"
      records("error") should contain only "[test-with-error] had an unexpected exception: foobar"

      await(control.drainAndShutdown()(microservice.system.dispatcher))
    }
  }

  it should "shutdown on error with no error topic configured" in {
    withRunningKafka {
      val microservice = new NioMicroservice[String, String]("test") {
        override def process(input: String): (String, String) = {
          throw new RuntimeException("foobar")
        }
      }
      val after = microservice.runUntilDone

      publishStringMessageToKafka("foo", "quux")

      val res = await(after.failed)

      res shouldBe a[RuntimeException]
      res.getMessage should equal("foobar")
    }
  }

  def await[T](x: Awaitable[T]): T = Await.result(x, Duration.Inf)

  // comment this region out to get kafka per test
  // region OneKafka
//  override def beforeAll(): Unit = {
//    EmbeddedKafka.start()
//  }
//
//  override def afterAll(): Unit = {
//    EmbeddedKafka.stop()
//  }
//
//  def withRunningKafka(body: => Any): Any = body
  // endregion
}
