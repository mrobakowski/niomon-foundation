package com.ubirch.niomon.util

import com.ubirch.niomon.base.NioMicroservice

// this is needed when the kafka serializer or deserialized depends somehow on the app's configuration (which is a part
// of the NioMicroservice.Context)
trait KafkaPayloadFactory[T] extends (NioMicroservice.Context => KafkaPayload[T])

object KafkaPayloadFactory {
  implicit def fromKafkaPayload[T: KafkaPayload]: KafkaPayloadFactory[T] = _ => KafkaPayload[T]
}
