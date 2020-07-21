package com.ubirch.niomon.base

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.kafka._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

/** This is what every Niomon microservice has to implement and plug into a [[NioMicroservice]] runtime */
abstract class NioMicroserviceLogic[I, O](runtime: NioMicroservice[I, O]) extends StrictLogging {
  /** @see [[NioMicroservice.outputTopics]] */
  final def outputTopics: Map[String, String] = runtime.outputTopics
  /** @see [[NioMicroservice.context]] */
  final def context: NioMicroservice.Context = runtime.context
  /** @see [[NioMicroservice.onlyOutputTopic]] */
  final def onlyOutputTopic: String = runtime.onlyOutputTopic
  /** @see [[NioMicroservice.config]] */
  final def config: Config = context.config
  /** @see [[NioMicroservice.stringifyException]] */
  final def stringifyException(e: Throwable, reqId: String): String = runtime.stringifyException(e, reqId)

  /** The business logic of the microservice */
  def processRecord(record: ConsumerRecord[String, I]): ProducerRecord[String, O]
}

object NioMicroserviceLogic {
  abstract class Simple[I, O](runtime: NioMicroservice[I, O]) extends NioMicroserviceLogic[I, O](runtime) {
    final def processRecord(record: ConsumerRecord[String, I]): ProducerRecord[String, O] = {
      val (output, topicKey) = process(record.value())
      record.toProducerRecord(topic = outputTopics(topicKey), value = output)
    }

    /** Simpler version of [[NioMicroserviceLogic.processRecord]]. You only get the value of the input record and you
     * return a tuple (output value, destination topic alias (see [[NioMicroservice.outputTopics]])) */
    def process(record: I): (O, String)
  }
}
