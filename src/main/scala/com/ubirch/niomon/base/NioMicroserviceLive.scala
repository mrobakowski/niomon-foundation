package com.ubirch.niomon.base

import java.time

import akka.Done
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer.{Control, DrainingControl}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Partition, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, SinkShape}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.Logger
import com.ubirch.kafka._
import com.ubirch.niomon.healthcheck.{Checks, HealthCheckServer}
import com.ubirch.niomon.util.{KafkaPayload, KafkaPayloadFactory, RetriableCommitter}
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import io.prometheus.client.{Counter, Summary}
import net.logstash.logback.argument.StructuredArguments.v
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{ProducerRecord, Producer => KProducer}
import org.apache.kafka.common.serialization._
import org.nustaq.serialization.FSTConfiguration
import org.redisson.Redisson
import org.redisson.api.{RMapCache, RedissonClient}
import org.redisson.codec.FstCodec
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

final class NioMicroserviceLive[Input, Output](
  val name: String,
  logicFactory: NioMicroservice[Input, Output] => NioMicroserviceLogic[Input, Output],
)(implicit
  inputPayloadFactory: KafkaPayloadFactory[Input],
  outputPayloadFactory: KafkaPayloadFactory[Output]
) extends NioMicroservice[Input, Output] {
  protected val logger: Logger =
    Logger(LoggerFactory.getLogger(getClass.getName + s"($name)"))
  val appConfig: Config = ConfigFactory.load()
  override val config: Config = appConfig.getConfig(name)

  val healthCheckServer: HealthCheckServer = {
    val s = new HealthCheckServer(Map(), Map())

    s.setLivenessCheck(Checks.process())
    s.setReadinessCheck(Checks.process())

    s.setLivenessCheck(Checks.notInitialized("business-logic"))
    s.setReadinessCheck(Checks.notInitialized("business-logic"))

    s.setReadinessCheck(Checks.notInitialized("kafka-consumer"))
    s.setReadinessCheck(Checks.notInitialized("kafka-success-producer"))
    s.setReadinessCheck(Checks.notInitialized("kafka-error-producer"))

    if (config.getBoolean("health-check.enabled")) {
      s.run(config.getInt("health-check.port"))
    }

    s
  }

  implicit val system: ActorSystem = ActorSystem(name)
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private val receivedMessagesCounter = Counter
    .build(s"received_messages_count", s"Number of kafka messages received")
    .register()
  private val successCounter = Counter
    .build(s"successes_count", s"Number of messages successfully processed")
    .register()
  private val failureCounter = Counter
    .build(s"failures_count", s"Number of messages unsuccessfully processed")
    .register()
  private val processingTimer = Summary
    .build(s"processing_time", s"Message processing time in seconds")
    .register()

  var caches: Vector[RMapCache[_, _]] = Vector()

  def purgeCaches(): Unit = {
    logger.info("purging caches")
    caches.foreach(_.clear())
    logger.debug(s"cache sizes after purging: [${caches.map(c => c.getName + " => " + c.size()).mkString("; ")}]")
  }

  lazy val redisson: RedissonClient = Redisson.create({
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
  })

  override val context = new NioMicroservice.Context(redisson, config, { c =>
    logger.debug("registering new cache")
    caches :+= c
    logger.debug(s"caches in total: ${caches.size}")
  })

  val inputTopics: Seq[String] = config.getStringList("kafka.topic.incoming").asScala
  override val outputTopics: Map[String, String] = config.getConfig("kafka.topic.outgoing").entrySet().asScala.map { e =>
    try {
      e.getKey -> e.getValue.unwrapped().asInstanceOf[String]
    } catch {
      case cce: ClassCastException => throw new RuntimeException("values in `kafka.topic.outgoing` must be string", cce)
    }
  }(scala.collection.breakOut)

  val errorTopic: Option[String] = Try(config.getString("kafka.topic.error")).toOption
  val failOnGraphException: Boolean = Try(config.getBoolean("failOnGraphException")).getOrElse(true)

  val kafkaUrl: String = config.getString("kafka.url")
  val consumerConfig: Config = system.settings.config.getConfig("akka.kafka.consumer")
  val producerConfig: Config = system.settings.config.getConfig("akka.kafka.producer")

  implicit val inputPayload: KafkaPayload[Try[Input]] = KafkaPayload.tryDeserializePayload(inputPayloadFactory(context))
  implicit val outputPayload: KafkaPayload[Output] = outputPayloadFactory(context)

  val consumerSettings: ConsumerSettings[String, Try[Input]] =
    ConsumerSettings(consumerConfig, new StringDeserializer, inputPayload.deserializer)
      .withBootstrapServers(kafkaUrl)
      .withGroupId(name)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      // timeout for closing the producer stage - by default it'll wait for commits for 30 seconds
      .withStopTimeout(Try(config.getDuration("kafka.stopTimeout")).getOrElse(time.Duration.ofSeconds(30)))

  val producerSettingsForSuccess: ProducerSettings[String, Output] =
    ProducerSettings(producerConfig, new StringSerializer, outputPayload.serializer)
      .withBootstrapServers(kafkaUrl)
  val kafkaProducerForSuccess: KProducer[String, Output] = producerSettingsForSuccess.createKafkaProducer()

  val producerSettingsForError: ProducerSettings[String, String] =
    ProducerSettings(producerConfig, new StringSerializer, new StringSerializer)
      .withBootstrapServers(kafkaUrl)
  val kafkaProducerForError: KProducer[String, String] = producerSettingsForError.createKafkaProducer()

  val kafkaSource: Source[ConsumerMsg, Consumer.Control] =
    Consumer.committableSource(consumerSettings, Subscriptions.topics(inputTopics: _*))

  val kafkaSuccessSink: Sink[ProducerMsg, Future[Done]] =
    Flow[ProducerMsg].map { msg: ProducerMsg =>
      msg.copy(record = msg.record.withExtraHeaders("previous-microservice" -> name))
    }.via(Producer.flexiFlow(producerSettingsForSuccess, kafkaProducerForSuccess))
      .map(_.passThrough)
      .toMat(RetriableCommitter.sink(CommitterSettings(system), 10, 1.5, 200))(Keep.right)

  val kafkaErrorSink: Sink[ProducerErr, Future[Done]] = errorTopic match {
    case Some(et) =>
      Producer.committableSink(producerSettingsForError, kafkaProducerForError)
        .contramap { errMsg: ProducerErr =>
          logger.error(s"error sink has received an exception, sending on [$et]", errMsg.record.value())
          val errRecordWithStatus = producerErrorRecordToStringRecord(errMsg.record, et)
          errMsg.copy(record = errRecordWithStatus)
        }
    case None =>
      RetriableCommitter.sink(CommitterSettings(system), 10, 1.5, 200).contramap { errMsg: ProducerErr =>
        val exception = errMsg.record.value()
        logger.error("error sink has received an exception", exception)
        if (failOnGraphException) {
          logger.error("failOnGraphException set to true, rethrowing")
          throw exception
        }

        errMsg.passThrough
      }
  }

  lazy val logic: NioMicroserviceLogic[Input, Output] = logicFactory(this)

  def processRecord(input: ConsumerRecord[String, Input]): ProducerRecord[String, Output] = logic.processRecord(input)

  def graph: RunnableGraph[DrainingControl[Done]] = {
    val bothDone: (Future[Done], Future[Done]) => Future[Done] = (f1, f2) =>
      Future.sequence(List(f1, f2)).map(_ => Done)

    val sink: Sink[Either[ProducerErr, ProducerMsg], Future[Done]] =
      Sink.fromGraph(GraphDSL.create(kafkaSuccessSink, kafkaErrorSink)(bothDone) { implicit builder =>
        (success, error) =>
          import GraphDSL.Implicits._
          val fanOut = builder.add(new Partition[Either[ProducerErr, ProducerMsg]](2, {
            case Right(_) => 0
            case Left(_) => 1
          }, eagerCancel = true))

          fanOut.out(0).map(_.right.get) ~> success
          fanOut.out(1).map(_.left.get) ~> error

          new SinkShape(fanOut.in)
      })

    kafkaSource.map { msg =>
      receivedMessagesCounter.inc()
      processingTimer.time { () =>
        Try {
          logger.info(s"$name is processing message with id [{}] and headers [{}]",
            v("requestId", msg.record.key()), v("headers", msg.record.headersScala.asJava))
          val msgHeaders = msg.record.headersScala
          if (msgHeaders.keys.map(_.toLowerCase).exists(_ == "x-niomon-purge-caches")) purgeCaches()

          val outputRecord: ProducerRecord[String, Output] = {
            val msgDeserializationErrorsRethrown: ConsumerRecord[String, Input] =
              msg.record.copy(value = msg.record.value().get)

            val res = processRecord(msgDeserializationErrorsRethrown)
            msgHeaders.get("x-niomon-force-reply-to") match {
              case Some(destination) => res.copy(topic = destination)
              case None => res
            }
          }
          successCounter.inc()

          new ProducerMsg(outputRecord, msg.committableOffset)
        }.toEither.left.map { e =>
          logger.error(s"$name errored while processing message with id [{}]", v("requestId", msg.record.key()), e)
          failureCounter.inc()
          val record = wrapThrowableInKafkaRecord(msg.record, e)

          new ProducerErr(record, msg.committableOffset)
        }
      }
    }
      .toMat(sink)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
  }

  def updateHealthChecks(kafkaControl: Control): Unit = {
    // business logic
    healthCheckServer.setLivenessCheck(Checks.ok("business-logic"))
    healthCheckServer.setReadinessCheck(Checks.ok("business-logic"))

    // kafka
    val kafkaReachable = Checks.kafkaNodesReachable(kafkaProducerForSuccess)
    healthCheckServer.setLivenessCheck(kafkaReachable)
    healthCheckServer.setReadinessCheck(kafkaReachable)

    healthCheckServer.setReadinessCheck(
      Checks.kafka("kafka-consumer", kafkaControl, connectionCountMustBeNonZero = true))
    healthCheckServer.setReadinessCheck(
      Checks.kafka("kafka-success-producer", kafkaProducerForSuccess, connectionCountMustBeNonZero = false))
    healthCheckServer.setReadinessCheck(
      Checks.kafka("kafka-error-producer", kafkaProducerForError, connectionCountMustBeNonZero = false))
  }

  def run: DrainingControl[Done] = {
    logger.info("starting prometheus server")
    DefaultExports.initialize()
    if (Try(appConfig.getBoolean("prometheus.enabled")).getOrElse(true)) {
      val _ = new HTTPServer(appConfig.getInt("prometheus.port"), true)
    }

    logger.info("starting business logic")
    val c = graph.run()
    updateHealthChecks(c)

    c
  }

  def runUntilDone: Future[Done] = {
    val control = run
    // flatMapped to `drainAndShutdown`, because bare `isShutdown` doesn't propagate errors
    control.isShutdown.flatMap { _ =>
      control.drainAndShutdown()
    }
  }

  def runUntilDoneAndShutdownProcess: Future[Nothing] = {
    // real impl is in the companion object, so we can conveniently change the execution context
    NioMicroserviceLive.runUntilDoneAndShutdownProcess(this)
  }
}

object NioMicroserviceLive {
  def apply[I, O](name: String, logicFactory: NioMicroservice[I, O] => NioMicroserviceLogic[I, O])
    (implicit ipf: KafkaPayloadFactory[I], opf: KafkaPayloadFactory[O]) =
    new NioMicroserviceLive[I, O](name, logicFactory)

  private def runUntilDoneAndShutdownProcess(that: NioMicroserviceLive[_, _]): Future[Nothing] = {
    // different execution context, because we cannot rely on actor system's dispatcher after it has been terminated
    import ExecutionContext.Implicits.global

    that.runUntilDone.transform(Success(_)) flatMap { done =>
      that.healthCheckServer.join()
      that.kafkaProducerForSuccess.close()
      that.kafkaProducerForError.close()
      that.system.terminate().map(_ => done)
    } transform { done =>
      done.flatten match {
        case Success(_) =>
          that.logger.info("microservice exited successfully")
          sys.exit(0)
        case Failure(e) =>
          that.logger.error("microservice exited with error", e)
          sys.exit(1)
      }
    }
  }
}
