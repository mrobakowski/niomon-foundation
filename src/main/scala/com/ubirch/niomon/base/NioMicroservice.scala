package com.ubirch.niomon.base

import akka.Done
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{GraphDSL, Keep, Partition, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, SinkShape}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.kafka._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

abstract class NioMicroservice[Input, Output](baseName: String)
                                             (implicit inputPayload: KafkaPayload[Input], outputPayload: KafkaPayload[Output])
  extends StrictLogging {
  implicit val system: ActorSystem = ActorSystem(baseName)
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val appConfig: Config = ConfigFactory.load() // TODO: should this just be system.settings.config?
  val config: Config = appConfig.getConfig(baseName)
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
      .withGroupId(baseName)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

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
    Producer.commitableSink(producerSettingsForSuccess)

  val kafkaFailureSink: Sink[ProducerErr, Future[Done]] = errorTopic match {
    case Some(et) =>
      Producer.commitableSink(producerSettingsForError)
        .contramap { errMsg: ProducerErr =>
          val exception = errMsg.record.value()
          logger.error(s"error sink has received an exception, sending on [$et]", exception)
          errMsg.copy(record = errMsg.record.copy(topic = et, value = exception.getMessage))
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
    val failureSinkEither = kafkaFailureSink.contramap { t: Either[ProducerErr, ProducerMsg] => t.left.get }

    val sink: Sink[Either[ProducerErr, ProducerMsg], Future[Done]] =
      Sink.fromGraph(GraphDSL.create(successSinkEither, failureSinkEither)(bothDone) { implicit builder =>
        (success, failure) =>
          import GraphDSL.Implicits._
          val fanOut = builder.add(new Partition[Either[ProducerErr, ProducerMsg]](2, {
            case Right(_) => 0
            case Left(_) => 1
          }, eagerCancel = true))

          fanOut.out(0) ~> success
          fanOut.out(1) ~> failure

          new SinkShape(fanOut.in)
      })

    kafkaSource
      .map { msg =>
        Try {
          val outputRecord = processRecord(msg.record)

          new ProducerMessage.Message[String, Output, ConsumerMessage.Committable](
            outputRecord,
            msg.committableOffset
          )
        }.toEither.left.map { e =>
          new ProducerMessage.Message[String, Throwable, ConsumerMessage.Committable](
            msg.record.toProducerRecord(topic = errorTopic.getOrElse("unused-topic"), value = e),
            msg.committableOffset
          )
        }
      }
      .toMat(sink)(Keep.both).mapMaterializedValue(DrainingControl.apply)
  }

  def run: DrainingControl[Done] = {
    graph.run()
  }

  def isDone: Future[Done] = {
    val control = run
    // flatMapped to `drainAndShutdown`, because bare `isShutdown` doesn't propagate errors
    control.isShutdown.flatMap { _ =>
      control.drainAndShutdown()
    }
  }
}

trait KafkaPayload[T] {
  def deserializer: Deserializer[T]

  def serializer: Serializer[T]
}

object KafkaPayload {
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
}
