package com.ubirch.niomon.base

import akka.Done
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{ConsumerMessage, ProducerMessage}
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.Config
import com.typesafe.scalalogging.{Logger, StrictLogging}
import com.ubirch.kafka._
import com.ubirch.niomon.base.NioMicroservice.{OM, WithHttpStatus}
import com.ubirch.niomon.cache.{RedisCache, TupledFunction}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import scala.annotation.tailrec
import scala.util.Try

trait NioMicroservice[I, O] {
  protected def logger: Logger

  def name: String

  def config: Config

  def outputTopics: Map[String, String]

  lazy val onlyOutputTopic: String = {
    if (outputTopics.size != 1)
      throw new IllegalStateException("you cannot use `onlyOutputTopic` with multiple output topics defined!")
    outputTopics.values.head
  }

  def errorTopic: Option[String]

  def context: NioMicroservice.Context

  def run: DrainingControl[Done]

  type ConsumerMsg = ConsumerMessage.CommittableMessage[String, Try[I]]
  type ProducerMsg = ProducerMessage.Message[String, O, ConsumerMessage.Committable]
  type ProducerErr = ProducerMessage.Message[String, Throwable, ConsumerMessage.Committable]

  def stringifyException(exception: Throwable, requestId: String): String = {
    import scala.collection.JavaConverters._

    val errMsg = exception.getMessage
    val errName = exception.getClass.getSimpleName

    @tailrec def causes(exc: Throwable, acc: Vector[String]): Vector[String] = {
      val cause = exc.getCause
      if (cause != null) {
        val errName = cause.getClass.getSimpleName
        causes(cause, acc :+ s"$errName: ${cause.getMessage}")
      } else {
        acc
      }
    }

    OM.writeValueAsString(Map(
      "error" -> s"$errName: $errMsg",
      "causes" -> causes(exception, Vector.empty).asJava,
      "microservice" -> name,
      "requestId" -> requestId
    ).asJava)
  }

  def wrapThrowableInKafkaRecord(record: ConsumerRecord[String, Try[I]], e: Throwable): ProducerRecord[String, Throwable] = {
    var prodRecord = record.toProducerRecord(topic = errorTopic.getOrElse("unused-topic"), value = e)
      .withExtraHeaders("previous-microservice" -> name)

    val headers = prodRecord.headersScala
    val httpStatusCodeKey = "http-status-code"

    if (!headers.contains(httpStatusCodeKey) || headers(httpStatusCodeKey)(0) <= '2') {
      prodRecord = prodRecord.withExtraHeaders(httpStatusCodeKey -> "500")
    }

    prodRecord
  }

  def producerErrorRecordToStringRecord(record: ProducerRecord[String, Throwable], errorTopic: String): ProducerRecord[String, String] = {
    val (exception, status) = record.value() match {
      case WithHttpStatus(s, cause) => (cause, Some(s))
      case e => (e, None)
    }

    logger.error(s"error sink has received an exception, sending on [$errorTopic]", exception)

    val stringifiedException = stringifyException(exception, record.key())

    val errRecord: ProducerRecord[String, String] = record.copy(topic = errorTopic, value = stringifiedException)
    val errRecordWithStatus = status match {
      case Some(s) => errRecord.withExtraHeaders("http-status-code" -> s.toString)
      case None => errRecord
    }

    errRecordWithStatus
  }
}

object NioMicroservice {
  def measureTime[R](code: => R, t: Long = System.nanoTime): (R, Long) = (code, System.nanoTime - t)

  class Context(getRedisCache: => RedisCache, val config: Config) extends StrictLogging {
    lazy val redisCache: RedisCache = getRedisCache

    //noinspection TypeAnnotation
    def cached[F](f: F)(implicit F: TupledFunction[F]) =
      redisCache.cached(f)
  }

  case class WithHttpStatus(status: Int, cause: Throwable) extends Exception(cause)

  private val OM = new ObjectMapper()
}