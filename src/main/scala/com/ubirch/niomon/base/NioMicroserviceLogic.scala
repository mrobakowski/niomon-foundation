package com.ubirch.niomon.base

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import com.ubirch.kafka._

abstract class NioMicroserviceLogic[I, O](runtime: NioMicroservice[I, O]) extends StrictLogging {
  final def outputTopics: Map[String, String] = runtime.outputTopics
  final def context: NioMicroservice.Context = runtime.context
  final def onlyOutputTopic: String = runtime.onlyOutputTopic
  final def config: Config = context.config
  def stringifyException(e: Throwable, reqId: String): String = runtime.stringifyException(e, reqId)
  def processRecord(input: ConsumerRecord[String, I]): ProducerRecord[String, O]
}

object NioMicroserviceLogic {
  abstract class Simple[I, O](runtime: NioMicroservice[I, O]) extends NioMicroserviceLogic[I, O](runtime) {
    final def processRecord(input: ConsumerRecord[String, I]): ProducerRecord[String, O] = {
      val (output, topicKey) = process(input.value())
      input.toProducerRecord(topic = outputTopics(topicKey), value = output)
    }

    def process(input: I): (O, String)
  }
}
