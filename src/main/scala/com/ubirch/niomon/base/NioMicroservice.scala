package com.ubirch.niomon.base

import java.util.concurrent.TimeUnit

import akka.Done
import akka.kafka.{ConsumerMessage, ProducerMessage}
import akka.kafka.scaladsl.Consumer.DrainingControl
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.Config
import com.typesafe.scalalogging.{Logger, StrictLogging}
import com.ubirch.niomon.base.NioMicroservice.{OM, WithHttpStatus}
import com.ubirch.niomon.util.TupledFunction
import com.ubirch.kafka._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.redisson.api.{RMapCache, RedissonClient}

import scala.annotation.tailrec
import scala.util.Try

trait NioMicroservice[I, O] {
  protected def logger: Logger
  def name: String
  def config: Config
  def outputTopics: Map[String, String]
  def context: NioMicroservice.Context
  def onlyOutputTopic: String
  def errorTopic: Option[String]

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

  class Context(getRedisson: => RedissonClient, val config: Config, registerCache: RMapCache[_, _] => Unit = { _ => }) extends StrictLogging {
    lazy val redisson: RedissonClient = getRedisson

    // This cache API is split in two steps (`cached(_).buildCache(_)`) to make type inference happy.
    // Originally it was just `cached(name)(function)`, but when `shouldCache` parameter was added after the `name`,
    // it screwed up type inference, because it was lexically before the `function`. And it is the `function` that has
    // the correct types for the type inference
    //noinspection TypeAnnotation
    def cached[F](f: F)(implicit F: TupledFunction[F]) = new CacheBuilder[F, F.Output](f)

    // V is here just to make type inference possible. V == tupledFunction.Output
    class CacheBuilder[F, V] private[NioMicroservice](f: F)(implicit val tupledFunction: TupledFunction[F]) {
      private val tupledF = tupledFunction.tupled(f)

      // for some reason, this doesn't really work with arbitrary key types, so we always use strings for keys
      def buildCache(name: String, shouldCache: V => Boolean = { _ => true })
        (implicit cacheKey: CacheKey[tupledFunction.TupledInput]): F = {
        val cache = redisson.getMapCache[String, tupledFunction.Output](name)
        registerCache(cache)

        val ttl = config.getDuration(s"$name.timeToLive")
        val maxIdleTime = config.getDuration(s"$name.maxIdleTime")

        tupledFunction.untupled { x: tupledFunction.TupledInput =>
          val (res, time) = measureTime {
            val key = cacheKey.key(x)
            val res = cache.get(key)

            if (res != null) {
              logger.debug(s"Cache hit in [$name] for key [$key]")
              res
            } else {
              logger.debug(s"Cache miss in [$name] for key [$key]")
              val freshRes = tupledF(x)
              if (shouldCache(freshRes.asInstanceOf[V])) {
                cache.fastPut(key, freshRes, ttl.toNanos, TimeUnit.NANOSECONDS, maxIdleTime.toNanos, TimeUnit.NANOSECONDS)
              }
              freshRes
            }
          }
          logger.debug(s"Cache lookup in [$name] took $time ns (~${Math.round(time / 1000000.0)} ms)")
          res
        }
      }
    }

  }

  trait CacheKey[-T] {
    def key(x: T): String
  }

  object CacheKey {

    implicit object ToStringKey extends CacheKey[Any] {
      override def key(x: Any): String = x.toString
    }

  }

  case class WithHttpStatus(status: Int, cause: Throwable) extends Exception(cause)

  private val OM = new ObjectMapper()
}