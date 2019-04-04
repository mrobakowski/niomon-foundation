package com.ubirch.niomon.util

import com.ubirch.kafka.{EnvelopeDeserializer, EnvelopeSerializer, MessageEnvelope}
import org.apache.kafka.common.serialization._

trait KafkaPayload[T] {
  def deserializer: Deserializer[T]

  def serializer: Serializer[T]
}

object KafkaPayload {
  implicit val StringKafkaPayload: KafkaPayload[String] = new KafkaPayload[String] {
    override def deserializer: Deserializer[String] = new StringDeserializer

    override def serializer: Serializer[String] = new StringSerializer
  }

  implicit val ByteArrayKafkaPayload: KafkaPayload[Array[Byte]] = new KafkaPayload[Array[Byte]] {
    override def deserializer: Deserializer[Array[Byte]] = new ByteArrayDeserializer

    override def serializer: Serializer[Array[Byte]] = new ByteArraySerializer
  }

  implicit val MessageEnvelopeKafkaPayload: KafkaPayload[MessageEnvelope] = new KafkaPayload[MessageEnvelope] {
    override def deserializer: Deserializer[MessageEnvelope] = EnvelopeDeserializer

    override def serializer: Serializer[MessageEnvelope] = EnvelopeSerializer
  }
}