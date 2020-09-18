package com.ubirch.niomon.base

import akka.Done
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{ConsumerMessage, ProducerMessage}
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.Config
import com.typesafe.scalalogging.{Logger, StrictLogging}
import com.ubirch.kafka._
import com.ubirch.niomon.base.NioMicroservice.{OM, WithHttpStatus}
import com.ubirch.niomon.cache.{DoesNotReturnFuture, RedisCache, ReturnsFuture, TupledFunction}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import scala.annotation.tailrec
import scala.util.Try

/**
 * Niomon microservice runtime. This library also includes the Live implementation of this. See foundation-mock for the
 * test implementation.
 */
trait NioMicroservice[I, O] {
  protected def logger: Logger

  /** name of the microservice */
  def name: String

  /**
   * <b>Microservice-specific</b> part of the config. By default it's the part of the application.conf under the key of
   * the same value as [[name]].
   */
  def config: Config

  /**
   * Config-dependent output topics with their aliases. Keys of the map are the aliases, values are actual kafka topics.
   */
  def outputTopics: Map[String, String]

  /**
   * You can use this if you are certain there's only one (non-error) output topic configured. Otherwise throws.
   */
  lazy val onlyOutputTopic: String = {
    if (outputTopics.size != 1)
      throw new IllegalStateException("you cannot use `onlyOutputTopic` with multiple output topics defined!")
    outputTopics.values.head
  }

  /** The topic to which serialized processing exceptions are sent if not [[scala.None]]. */
  def errorTopic: Option[String]

  /** Parts of the runtime exposed to the [[NioMicroserviceLogic]] instance. */
  def context: NioMicroservice.Context

  /** Starts the NioMicroservice and its underlying `akka-streaming` graph. */
  def run: DrainingControl[Done]

  // bunch of useful type aliases
  type ConsumerMsg = ConsumerMessage.CommittableMessage[String, Try[I]]
  type ProducerMsg = ProducerMessage.Message[String, O, ConsumerMessage.Committable]
  type ProducerErr = ProducerMessage.Message[String, Throwable, ConsumerMessage.Committable]

  /**
   * Tries its best to turn an arbitrary exception into a useful message you may send to different microservices or to
   * the user.
   */
  def stringifyException(exception: Throwable, requestId: String): String = {
    import scala.collection.JavaConverters._

    val errMsg = exception.getMessage
    val errName = exception.getClass.getSimpleName

    @tailrec def causes(exc: Throwable, acc: Vector[String]): Vector[String] = {
      val cause = exc.getCause
      if (cause != null && cause != exc) {
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

  /**
   * Creates a [[ProducerRecord]] with all the same metadata (headers, etc.) as the `record` and `e` as its value.
   * If no `http-status-code` header is found in the source record, or if its value is 2XX or less, it sets the header
   * to 500.
   */
  def wrapThrowableInKafkaRecord(record: ConsumerRecord[String, Try[I]], e: Throwable): ProducerRecord[String, Throwable] = {
    var prodRecord = record
      .toProducerRecord(topic = errorTopic.getOrElse("unused-topic"), value = e)
      .withExtraHeaders("previous-microservice" -> name)

    val headers = prodRecord.headersScala
    val httpStatusCodeKey = "http-status-code"

    if (!headers.contains(httpStatusCodeKey) || headers(httpStatusCodeKey)(0) <= '2') {
      prodRecord = prodRecord.withExtraHeaders(httpStatusCodeKey -> "500")
    }

    prodRecord
  }

  /**
   * Transforms a [[ProducerRecord]][ [[String]], [[Throwable]] ] into a [[ProducerRecord]][ [[String]], [[String]] ].
   * If the inner exception is a [[WithHttpStatus]], it is unwrapped and the appropriate http status header is set.
   */
  def producerErrorRecordToStringRecord(record: ProducerRecord[String, Throwable], errorTopic: String): ProducerRecord[String, String] = {
    val (exception, status, code) = record.value() match {
      case WithHttpStatus(s, cause, code) => (cause, Some(s), code)
      case e => (e, None, None)
    }

    logger.error(s"error sink has received an exception, sending on [$errorTopic]", exception)

    val requestId = record.requestIdHeader().orNull

    val stringifiedException = stringifyException(exception, requestId)

    val errRecord: ProducerRecord[String, String] = record.copy(topic = errorTopic, value = stringifiedException)
    val errRecordWithStatus = status match {
      case Some(s) =>
        val headers = ("http-status-code" -> s.toString) +: code.toList.map(c => "x-code" -> c.toString)
        errRecord.withExtraHeaders(headers : _ *)
      case None => errRecord
    }

    errRecordWithStatus
  }
}

object NioMicroservice {
  /** Runs a block of code and returns its result along with the time it took */
  def measureTime[R](code: => R, t: Long = System.nanoTime): (R, Long) = (code, System.nanoTime - t)

  /**
   * Parts of the runtime exposed to the [[NioMicroserviceLogic]] instance.
   * @param config @see [[NioMicroservice.config]]
   */
  class Context(
    getRedisCache: => RedisCache,
    val config: Config
  ) extends StrictLogging {
    lazy val redisCache: RedisCache = getRedisCache

    /** Creates a caching version of a function `f`. Its parameters and the return value better be plain-old objects! */
    //noinspection TypeAnnotation
    def cached[F](f: F)(implicit F: TupledFunction[F], ev: DoesNotReturnFuture[F]) =
      redisCache.cached(f)

    /**
     * Like [[cached]], but supports functions returning [[scala.concurrent.Future]]s of plain-old objects.
     * @see [[cached]]
     */
    //noinspection TypeAnnotation
    def cachedF[F](f: F)(implicit F: TupledFunction[F], ev: ReturnsFuture[F]) =
      redisCache.cachedF(f)
  }

  case class WithHttpStatus(status: Int, cause: Throwable, code: Option[Int] =  None) extends Exception(cause)

  // used only for stringifying the exceptions
  private val OM = new ObjectMapper()
}
