package com.ubirch.niomon.base

import java.time.Duration

import cakesolutions.kafka.{KafkaConsumer, KafkaProducer}
import cakesolutions.kafka.testkit.KafkaServer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer => JKafkaConsumer}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

class NioMicroserviceTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  var kafka: KafkaServer = _
  var producer: KafkaProducer[String, String] = _
  var consumer: JKafkaConsumer[String, String] = _

  "NioMicroservice" should "work" in {
    consumer.subscribe(List("bar").asJava)

    //noinspection TypeAnnotation
    val microservice = new NioMicroservice[String, String]("test") {
      var n = 0

      override def process(input: String): (String, String) = {
        n += 1
        s"foobar$n" -> "default"
      }
    }
    val control = microservice.run

    producer.send(new ProducerRecord("foo", "1", "quux"))
    producer.send(new ProducerRecord("foo", "2", "quux"))
    producer.send(new ProducerRecord("foo", "3", "quux"))

    var records = List[ConsumerRecord[String, String]]()

    while (records.size < 3) {
      val res = consumer.poll(Duration.ofSeconds(2))
      consumer.commitAsync()
      records = records ::: res.iterator().asScala.toList
    }

    records.size should equal(3)
    records.map(_.value()) should contain allOf("foobar1", "foobar2", "foobar3")

    Await.result(control.drainAndShutdown()(microservice.system.dispatcher), 1.minute)
  }

  it should "shutdown on error with no error topic configured" in {
    //noinspection TypeAnnotation
    val microservice = new NioMicroservice[String, String]("test") {
      override def process(input: String): (String, String) = {
        throw new RuntimeException("foobar")
      }
    }
    val after = microservice.isDone

    producer.send(new ProducerRecord("foo", "1", "quux"))

    val res = Await.result(after.failed, 1.minute)

    res shouldBe a[RuntimeException]
    res.getMessage should equal("foobar")
  }

  override protected def beforeEach(): Unit = {
    kafka = new KafkaServer(kafkaPort = 9092)
    kafka.startup()
    consumer = KafkaConsumer(KafkaConsumer.Conf(new StringDeserializer, new StringDeserializer, groupId = "nio-test"))
    producer = KafkaProducer(KafkaProducer.Conf(new StringSerializer, new StringSerializer))
  }

  override protected def afterEach(): Unit = {
    producer.close()
    consumer.close()
    kafka.close()
    kafka = null
  }
}
