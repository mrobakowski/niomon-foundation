package com.ubirch.niomon.base

import java.time
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{GraphDSL, Keep, Partition, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, SinkShape}
import com.niomon.util.{KafkaPayload, TupledFunction}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.kafka._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._
import org.nustaq.serialization.FSTConfiguration
import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.codec.FstCodec

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

abstract class NioMicroservice[Input, Output](name: String)
                                             (implicit inputPayload: KafkaPayload[Input],
                                              outputPayload: KafkaPayload[Output])
  extends StrictLogging {
  implicit val system: ActorSystem = ActorSystem(name)
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val appConfig: Config = ConfigFactory.load() // TODO: should this just be system.settings.config?
  val config: Config = appConfig.getConfig(name)
  val inputTopics: Seq[String] = config.getStringList("kafka.topic.incoming").asScala
  val outputTopics: Map[String, String] = config.getConfig("kafka.topic.outgoing").entrySet().asScala.map { e =>
    try {
      e.getKey -> e.getValue.unwrapped().asInstanceOf[String]
    } catch {
      case cce: ClassCastException => throw new RuntimeException("values in `kafka.topic.outgoing` must be string", cce)
    }
  }(scala.collection.breakOut)
  val errorTopic: Option[String] = Try(config.getString("kafka.topic.error")).toOption
  val failOnGraphException: Boolean = Try(config.getBoolean("failGraphOnException")).getOrElse(true)

  val kafkaUrl: String = config.getString("kafka.url")
  val consumerConfig: Config = system.settings.config.getConfig("akka.kafka.consumer")
  val producerConfig: Config = system.settings.config.getConfig("akka.kafka.producer")

  val consumerSettings: ConsumerSettings[String, Input] =
    ConsumerSettings(consumerConfig, new StringDeserializer, inputPayload.deserializer)
      .withBootstrapServers(kafkaUrl)
      .withGroupId(name)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      // timeout for closing the producer stage - by default it'll wait for commits for 30 seconds
      .withStopTimeout(Try(config.getDuration("kafka.stopTimeout")).getOrElse(time.Duration.ofSeconds(30)))

  val producerSettingsForSuccess: ProducerSettings[String, Output] =
    ProducerSettings(producerConfig, new StringSerializer, outputPayload.serializer)
      .withBootstrapServers(kafkaUrl)

  val producerSettingsForError: ProducerSettings[String, String] =
    ProducerSettings(producerConfig, new StringSerializer, new StringSerializer)
      .withBootstrapServers(kafkaUrl)

  final type ConsumerMsg = ConsumerMessage.CommittableMessage[String, Input]
  final type ProducerMsg = ProducerMessage.Message[String, Output, ConsumerMessage.Committable]
  final type ProducerErr = ProducerMessage.Message[String, Throwable, ConsumerMessage.Committable]

  val kafkaSource: Source[ConsumerMsg, Consumer.Control] =
    Consumer.committableSource(consumerSettings, Subscriptions.topics(inputTopics: _*))

  val kafkaSuccessSink: Sink[ProducerMsg, Future[Done]] =
    Producer.committableSink(producerSettingsForSuccess).contramap { msg: ProducerMsg =>
      msg.copy(record = msg.record.withExtraHeaders("previous-microservice" -> name))
    }

  val kafkaErrorSink: Sink[ProducerErr, Future[Done]] = errorTopic match {
    case Some(et) =>
      Producer.committableSink(producerSettingsForError)
        .contramap { errMsg: ProducerErr =>
          val exception = errMsg.record.value()
          logger.error(s"error sink has received an exception, sending on [$et]", exception)
          errMsg.copy(record = errMsg.record.copy(topic = et, value =
            s"[$name] had an unexpected exception: " + exception.getMessage))
        }
    case None =>
      Sink.foreach { errMsg: ProducerErr =>
        val exception = errMsg.record.value()
        logger.error("error sink has received an exception", exception)
        if (failOnGraphException) {
          logger.error("failOnGraphException set to true, rethrowing")
          throw exception
        }
      }
  }

  def processRecord(input: ConsumerRecord[String, Input]): ProducerRecord[String, Output] = {
    val (output, topicKey) = process(input.value())
    input.toProducerRecord(topic = outputTopics(topicKey), value = output)
  }

  def process(input: Input): (Output, String) = throw new NotImplementedError(
    "at least one of {process, processRecord} must be overridden")

  def graph: RunnableGraph[DrainingControl[Done]] = {
    val bothDone: (Future[Done], Future[Done]) => Future[Done] = (f1, f2) =>
      Future.sequence(List(f1, f2)).map(_ => Done)

    val successSinkEither = kafkaSuccessSink.contramap { t: Either[ProducerErr, ProducerMsg] => t.right.get }
    val errorSinkEither = kafkaErrorSink.contramap { t: Either[ProducerErr, ProducerMsg] => t.left.get }

    val sink: Sink[Either[ProducerErr, ProducerMsg], Future[Done]] =
      Sink.fromGraph(GraphDSL.create(successSinkEither, errorSinkEither)(bothDone) { implicit builder =>
        (success, error) =>
          import GraphDSL.Implicits._
          val fanOut = builder.add(new Partition[Either[ProducerErr, ProducerMsg]](2, {
            case Right(_) => 0
            case Left(_) => 1
          }, eagerCancel = true))

          fanOut.out(0) ~> success
          fanOut.out(1) ~> error

          new SinkShape(fanOut.in)
      })

    kafkaSource.map { msg =>
      Try {
        logger.info(s"$name is processing message with id [${msg.record.key()}]...")
        val outputRecord = processRecord(msg.record)
        logger.info(s"$name successfully processed message with id [${msg.record.key()}]")

        new ProducerMsg(
          outputRecord,
          msg.committableOffset
        )
      }.toEither.left.map { e =>
        logger.error(s"$name errored while processing message with id [${msg.record.key()}]")

        new ProducerErr(
          msg.record.toProducerRecord(topic = errorTopic.getOrElse("unused-topic"), value = e)
            .withExtraHeaders("previous-microservice" -> name),
          msg.committableOffset
        )
      }
    }.toMat(sink)(Keep.both).mapMaterializedValue(DrainingControl.apply)
  }

  def run: DrainingControl[Done] = {
    graph.run()
  }

  def runUntilDone: Future[Done] = {
    val control = run
    // flatMapped to `drainAndShutdown`, because bare `isShutdown` doesn't propagate errors
    control.isShutdown.flatMap { _ =>
      control.drainAndShutdown()
    }
  }

  def runUntilDoneAndShutdownProcess: Future[Done] = {
    // real impl is in the companion object, so we can conveniently change the execution context
    NioMicroservice.runUntilDoneAndShutdownProcess(this)
  }

  lazy val redisson: RedissonClient = Redisson.create(
    {
      val conf = Try(appConfig.getConfig("redisson"))
        .map(_.root().render(ConfigRenderOptions.concise()))
        .map(org.redisson.config.Config.fromJSON)
        .getOrElse(new org.redisson.config.Config())

      // force the FST serializer to use serialize everything, because we sometimes want to store POJOs which
      // aren't `Serializable`
      if (conf.getCodec == null || conf.getCodec.isInstanceOf[FstCodec]) {
        conf.setCodec(new FstCodec(FSTConfiguration.createDefaultConfiguration().setForceSerializable(true)))
      }

      conf
    }
  )

  val context = new NioMicroservice.Context(redisson, config)
}

object NioMicroservice {
  private def runUntilDoneAndShutdownProcess(that: NioMicroservice[_, _]): Future[Done] = {
    // different execution context, because we cannot rely on actor system's dispatcher after it has been terminated
    import ExecutionContext.Implicits.global

    that.runUntilDone.transform(Success(_)) flatMap { done =>
      that.system.terminate().map(_ => done)
    } transform { done =>
      done.flatten match {
        case Success(_) =>
          that.logger.info("Exiting app successfully")
          System.exit(0)
        case Failure(e) =>
          that.logger.error("Exiting app after error", e)
          System.exit(1)
      }

      //noinspection NotImplementedCode
      ??? // unreachable
    }
  }

  def measureTime[R](code: => R, t: Long = System.nanoTime): (R, Long) = (code, System.nanoTime - t)

  class Context(getRedisson: => RedissonClient, val config: Config) extends StrictLogging {
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

}
