package com.ubirch.niomon.base

import java.util.concurrent.TimeUnit

import akka.Done
import akka.kafka.scaladsl.Consumer.DrainingControl
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.niomon.util.TupledFunction
import org.redisson.api.{RMapCache, RedissonClient}

trait NioMicroservice[I, O] {
  def config: Config
  def outputTopics: Map[String, String]
  def context: NioMicroservice.Context
  def onlyOutputTopic: String

  def run: DrainingControl[Done]
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

}