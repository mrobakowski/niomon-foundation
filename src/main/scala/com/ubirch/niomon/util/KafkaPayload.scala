package com.ubirch.niomon.util

import java.util

import com.ubirch.kafka.{EnvelopeDeserializer, EnvelopeSerializer, MessageEnvelope}
import org.apache.kafka.common.serialization._

import scala.util.Try

trait KafkaPayload[T] {
  def deserializer: Deserializer[T]

  def serializer: Serializer[T]
}

object KafkaPayload {
  def apply[T: KafkaPayload]: KafkaPayload[T] = implicitly[KafkaPayload[T]]

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

  abstract class EitherKafkaPayload[L: KafkaPayload, R: KafkaPayload] extends KafkaPayload[Either[L, R]] {
    private val leftSerializer = KafkaPayload[L].serializer
    private val rightSerializer = KafkaPayload[R].serializer

    override def serializer: Serializer[Either[L, R]] = new Serializer[Either[L, R]] {
      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
        leftSerializer.configure(configs, isKey)
        rightSerializer.configure(configs, isKey)
      }

      override def serialize(topic: String, data: Either[L, R]): Array[Byte] = {
        data match {
          case Left(value) => leftSerializer.serialize(topic, value)
          case Right(value) => rightSerializer.serialize(topic, value)
        }
      }

      override def close(): Unit = {
        leftSerializer.close()
        rightSerializer.close()
      }
    }
  }

  def tryBothEitherKafkaPayload[L: KafkaPayload, R: KafkaPayload]: KafkaPayload[Either[L, R]] = new EitherKafkaPayload[L, R] {
    override def deserializer: Deserializer[Either[L, R]] = new Deserializer[Either[L, R]] {
      private val leftDeserializer = KafkaPayload[L].deserializer
      private val rightDeserializer = KafkaPayload[R].deserializer

      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
        leftDeserializer.configure(configs, isKey)
        rightDeserializer.configure(configs, isKey)
      }

      override def deserialize(topic: String, data: Array[Byte]): Either[L, R] = {
        Try(Left(leftDeserializer.deserialize(topic, data)))
          .getOrElse(Right(rightDeserializer.deserialize(topic, data)))
      }

      override def close(): Unit = {
        leftDeserializer.close()
        rightDeserializer.close()
      }
    }
  }

  def topicBasedEitherKafkaPayload[L: KafkaPayload, R: KafkaPayload](decide: String => Either[Unit, Unit]): KafkaPayload[Either[L, R]] = new EitherKafkaPayload[L, R] {
    override def deserializer: Deserializer[Either[L, R]] = new Deserializer[Either[L, R]] {
      private val leftDeserializer = KafkaPayload[L].deserializer
      private val rightDeserializer = KafkaPayload[R].deserializer

      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
        leftDeserializer.configure(configs, isKey)
        rightDeserializer.configure(configs, isKey)
      }

      override def deserialize(topic: String, data: Array[Byte]): Either[L, R] = {
        decide(topic) match {
          case Left(_) => Left(leftDeserializer.deserialize(topic, data))
          case Right(_) => Right(rightDeserializer.deserialize(topic, data))
        }
      }

      override def close(): Unit = {
        leftDeserializer.close()
        rightDeserializer.close()
      }
    }
  }

  def tryDeserializePayload[T](inner: KafkaPayload[T]): KafkaPayload[Try[T]] = {
    new KafkaPayload[Try[T]] {
      override def deserializer: Deserializer[Try[T]] = tryDeserializer(inner.deserializer)

      override def serializer: Serializer[Try[T]] = successSerializer(inner.serializer)
    }
  }

  private def tryDeserializer[T](deserializer: Deserializer[T]): Deserializer[Try[T]] = {
    new Deserializer[Try[T]] {
      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = deserializer.configure(configs, isKey)

      override def deserialize(topic: String, data: Array[Byte]): Try[T] = Try(deserializer.deserialize(topic, data))

      override def close(): Unit = deserializer.close()
    }
  }

  private def successSerializer[T](serializer: Serializer[T]): Serializer[Try[T]] = {
    new Serializer[Try[T]] {
      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = serializer.configure(configs, isKey)

      override def serialize(topic: String, data: Try[T]): Array[Byte] = {
        serializer.serialize(topic, data.getOrElse(throw data.failed.get))
      }

      override def close(): Unit = serializer.close()
    }
  }
}