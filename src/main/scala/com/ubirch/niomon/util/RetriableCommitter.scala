package com.ubirch.niomon.util

import akka.actor.ActorSystem
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.{Committable, CommittableOffsetBatch}
import akka.pattern.after
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.{Done, NotUsed}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.common.errors.RetriableException

import scala.concurrent.Future
import scala.concurrent.duration._

/** Like [[akka.kafka.scaladsl.Committer]], but can retry. */
object RetriableCommitter extends StrictLogging {
  def flow(settings: CommitterSettings, retries: Int, backoffFactor: Double = 1.5, initialBackoffMs: Int = 200, maxBackoffMs: Int = 10000)
    (implicit as: ActorSystem): Flow[Committable, Done, NotUsed] =
    Flow[Committable]
      // Not very efficient, ideally we should merge offsets instead of grouping them
      .groupedWeightedWithin(settings.maxBatch, settings.maxInterval)(_.batchSize)
      .map(CommittableOffsetBatch.apply)
      .mapAsync(settings.parallelism) { offsetBatch =>
        // The contents of this closure is what is different between this committer and [[akka.kafka.scaladsl.Committer]]
        import as.dispatcher // execution context for the .recoverWith

        def doCommit(retriesLeft: Int, currentBackoff: Int): Future[Done] = offsetBatch.commitScaladsl()
          .recoverWith {
            case ex: RetriableException if retriesLeft > 0 =>
              logger.warn(
                s"Retrying batch commit in $currentBackoff ms ($retriesLeft retries left); " +
                  "failed due to the following exception:", ex
              )
              after(currentBackoff.milliseconds, as.scheduler) {
                doCommit(retriesLeft - 1, Math.min(Math.ceil(currentBackoff * backoffFactor).toInt, maxBackoffMs))
              }
            case commitFailed: CommitFailedException =>
              logger.error(
                "Batch commit failed. " +
                  s"The following offsets won't ever be committed by this microservice: [${offsetBatch.offsets()}]. " +
                  "Caused by:", commitFailed
              )
              // This is an unrecoverable failure, but restarting the service wouldn't fix anything.
              // might as well just continue
              Future.successful(Done)
            case otherException =>
              logger.error("Could not commit the batch due to a weird exception:", otherException)
              Future.failed(otherException)
          }

        doCommit(retries, initialBackoffMs)
      }

  def sink(
    settings: CommitterSettings,
    retries: Int,
    backoffFactor: Double = 1.5,
    initialBackoffMs: Int = 200,
    maxBackoffMs: Int = 10000
  )(implicit as: ActorSystem): Sink[Committable, Future[Done]] =
    flow(settings, retries, backoffFactor, initialBackoffMs, maxBackoffMs)
      .toMat(Sink.ignore)(Keep.right)
}
