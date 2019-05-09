package com.ubirch.niomon.util

import com.ubirch.niomon.base.NioMicroservice

trait KafkaPayloadFactory[T] extends (NioMicroservice.Context => KafkaPayload[T])

object KafkaPayloadFactory {
  implicit def fromKafkaPayload[T: KafkaPayload]: KafkaPayloadFactory[T] = _ => KafkaPayload[T]
}
