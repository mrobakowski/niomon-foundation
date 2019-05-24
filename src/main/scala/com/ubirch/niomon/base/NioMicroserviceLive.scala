package com.ubirch.niomon.base

import java.time

import akka.Done
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Committer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{GraphDSL, Keep, Partition, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, SinkShape}
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.kafka._
import com.ubirch.niomon.base.NioMicroservice.WithHttpStatus
import com.ubirch.niomon.util.{KafkaPayload, KafkaPayloadFactory}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._
import org.nustaq.serialization.FSTConfiguration
import org.redisson.Redisson
import org.redisson.api.{RMapCache, RedissonClient}
import org.redisson.codec.FstCodec

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

final class NioMicroserviceLive[Input, Output](
  name: String,
  logicFactory: NioMicroservice[Input, Output] => NioMicroserviceLogic[Input, Output]
)(implicit
  inputPayloadFactory: KafkaPayloadFactory[Input],
  outputPayloadFactory: KafkaPayloadFactory[Output]
) extends NioMicroservice[Input, Output] with StrictLogging {
  implicit val system: ActorSystem = ActorSystem(name)
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val appConfig: Config = ConfigFactory.load() // TODO: should this just be system.settings.config?
  override val config: Config = appConfig.getConfig(name)

  var caches: Vector[RMapCache[_, _]] = Vector()

  def purgeCaches(): Unit = {
    logger.info("purging caches!")
    caches.foreach(_.clear())
    logger.debug(s"cache sizes after purging: [${caches.map(c => c.getName + " => " + c.size()).mkString("; ")}]")
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

  override lazy val onlyOutputTopic: String = {
    if (outputTopics.size != 1)
      throw new IllegalStateException("you cannot use `onlyOutputTopic` with multiple output topics defined!")
    outputTopics.values.head
  }

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

  val producerSettingsForError: ProducerSettings[String, String] =
    ProducerSettings(producerConfig, new StringSerializer, new StringSerializer)
      .withBootstrapServers(kafkaUrl)

  type ConsumerMsg = ConsumerMessage.CommittableMessage[String, Try[Input]]
  type ProducerMsg = ProducerMessage.Message[String, Output, ConsumerMessage.Committable]
  type ProducerErr = ProducerMessage.Message[String, Throwable, ConsumerMessage.Committable]

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
          val (exception, status) = errMsg.record.value() match {
            case WithHttpStatus(s, cause) => (cause, Some(s))
            case e => (e, None)
          }

          logger.error(s"error sink has received an exception, sending on [$et]", exception)

          val stringifiedException = stringifyException(exception, errMsg.record.key())

          val errRecord: ProducerRecord[String, String] = errMsg.record.copy(topic = et, value = stringifiedException)
          val errRecordWithStatus = status match {
            case Some(s) => errRecord.withExtraHeaders("http-status-code" -> s.toString)
            case None => errRecord
          }

          errMsg.copy(record = errRecordWithStatus)
        }
    case None =>
      Committer.sink(CommitterSettings(system)).contramap { errMsg: ProducerErr =>
        val exception = errMsg.record.value()
        logger.error("error sink has received an exception", exception)
        if (failOnGraphException) {
          logger.error("failOnGraphException set to true, rethrowing")
          throw exception
        }

        errMsg.passThrough
      }
  }

  private val OM = new ObjectMapper()

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

  lazy val logic: NioMicroserviceLogic[Input, Output] = logicFactory(this)

  def processRecord(input: ConsumerRecord[String, Input]): ProducerRecord[String, Output] = logic.processRecord(input)

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
        logger.info(s"$name is processing message with id [${msg.record.key()}] and headers [${msg.record.headersScala}]...")
        val msgHeaders = msg.record.headersScala
        if (msgHeaders.keys.map(_.toLowerCase).exists(_ == "x-niomon-purge-caches")) purgeCaches()

        val outputRecord = {
          val msgDeserializationErrorsRethrown = msg.record.copy(value = msg.record.value().get)
          val res = processRecord(msgDeserializationErrorsRethrown)
          msgHeaders.get("x-niomon-force-reply-to") match {
            case Some(destination) => res.copy(topic = destination)
            case None => res
          }
        }
        logger.info(s"$name successfully processed message with id [${outputRecord.key()}]")

        new ProducerMsg(
          outputRecord,
          msg.committableOffset
        )
      }.toEither.left.map { e =>
        logger.error(s"$name errored while processing message with id [${msg.record.key()}]")

        var prodRecord = msg.record.toProducerRecord(topic = errorTopic.getOrElse("unused-topic"), value = e)
          .withExtraHeaders("previous-microservice" -> name)

        val headers = prodRecord.headersScala
        val httpStatusCodeKey = "http-status-code"

        if (!headers.contains(httpStatusCodeKey) || headers(httpStatusCodeKey)(0) <= '2') {
          prodRecord = prodRecord.withExtraHeaders(httpStatusCodeKey -> "500")
        }

        new ProducerErr(prodRecord, msg.committableOffset)
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
    NioMicroserviceLive.runUntilDoneAndShutdownProcess(this)
  }
}

object NioMicroserviceLive {
  def apply[I, O](name: String, logicFactory: NioMicroservice[I, O] => NioMicroserviceLogic[I, O])
    (implicit ipf: KafkaPayloadFactory[I], opf: KafkaPayloadFactory[O]) = new NioMicroserviceLive[I, O](name, logicFactory)

  private def runUntilDoneAndShutdownProcess(that: NioMicroserviceLive[_, _]): Future[Nothing] = {
    // different execution context, because we cannot rely on actor system's dispatcher after it has been terminated
    import ExecutionContext.Implicits.global

    that.runUntilDone.transform(Success(_)) flatMap { done =>
      that.system.terminate().map(_ => done)
    } transform { done =>
      done.flatten match {
        case Success(_) =>
          that.logger.info("Exiting app successfully")
          sys.exit(0)
        case Failure(e) =>
          that.logger.error("Exiting app after error", e)
          sys.exit(1)
      }
    }
  }
}
