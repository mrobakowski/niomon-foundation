package com.ubirch.niomon.healthcheck

import java.util.Collections

import com.avsystem.commons.rpc.AsRaw
import io.prometheus.client.CollectorRegistry
import io.udash.rest.openapi.adjusters.adjustSchema
import io.udash.rest.openapi.{DataType, Info, RefOr, RestSchema, Schema, Server}
import io.udash.rest.raw.{HttpBody, RestResponse}
import io.udash.rest.{DefaultRestApiCompanion, GET, RestDataCompanion, RestDataWrapperCompanion}
import io.udash.rest.raw.JsonValue
import org.json4s.JsonAST.{JDouble, JObject, JString, JValue}
import org.json4s.JsonDSL
import org.json4s.jackson.JsonMethods

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._

case class CheckResult(checkName: String, success: Boolean, payload: JValue)

class HealthCheckServer(
  var livenessChecks: Map[String, () => Future[CheckResult]],
  var readinessChecks: Map[String, () => Future[CheckResult]],
  swaggerBaseUrl: String
) extends HealthCheckApi {
  implicit val ec: ExecutionContext = ExecutionContext.global

  private def doCheck(checks: Map[String, () => Future[CheckResult]]): Future[(Boolean, JValue)] = {
    Future.sequence(checks.values.map(_ ()))
      .map { checks =>
        checks.foldLeft((true, JObject())) { case ((success, o), check) =>
          (success && check.success, o.merge(JsonDSL.pair2jvalue((check.checkName, check.payload))))
        }
      }
  }

  def setLivenessCheck(name: String)(checkFn: () => Future[CheckResult]): Unit = {
    livenessChecks += name -> checkFn
  }

  def setLivenessCheck(nameAndCheckFn: (String, () => Future[CheckResult])): Unit = {
    livenessChecks += nameAndCheckFn
  }

  def setReadinessCheck(name: String)(checkFn: () => Future[CheckResult]): Unit = {
    readinessChecks += name -> checkFn
  }

  def setReadinessCheck(nameAndCheckFn: (String, () => Future[CheckResult])): Unit = {
    readinessChecks += nameAndCheckFn
  }

  private def endpoint(checks: Map[String, () => Future[CheckResult]]): Future[HealthCheckResponse] = {
    doCheck(checks).map { case (success, payload) =>
      val serializedPayload = JsonValue(JsonMethods.compact(payload))
      if (success) {
        HealthCheckSuccess(serializedPayload)
      } else {
        HealthCheckFailure(serializedPayload)
      }
    }
  }

  override def live(): Future[HealthCheckResponse] = endpoint(livenessChecks)

  override def ready(): Future[HealthCheckResponse] = endpoint(readinessChecks)

  private var server: JettyServer = _

  def run(port: Int): Unit = {
    if (server != null) join()

    server = new JettyServer(this, HealthCheckApi.openapiMetadata.openapi(
      Info("Health Check API", "1.0.0"),
      servers = List(Server(swaggerBaseUrl))
    ), port)

    server.start()
  }

  def join(): Unit = if (server != null) server.join()
}

object Checks {
  def notInitialized(name: String): (String, () => Future[CheckResult]) = {
    (name, () => Future.successful(CheckResult(name, success = false, JObject("status" -> JString("not initialized")))))
  }

  def ok(name: String): (String, () => Future[CheckResult]) = {
    (name, () => Future.successful(CheckResult(name, success = true, JObject("status" -> JString("ok")))))
  }

  def system(): (String, () => Future[CheckResult]) = {
    ("system", { () =>
      val dataPoints = Collections.list(CollectorRegistry.defaultRegistry.filteredMetricFamilySamples(Set(
        "process_cpu_seconds_total",
        "process_start_time_seconds",
        "process_virtual_memory_bytes",
        "process_resident_memory_bytes",
        "jvm_threads_current"
      ).asJava)).asScala

      val json = dataPoints.foldRight(JObject())((samples, jo) => jo.merge(
        JObject(samples.name.replaceAll("_", "-") -> JDouble(samples.samples.asScala.last.value))
      ))

      Future.successful(CheckResult("system", true, json))
    })
  }
}

@adjustSchema(HealthCheckResponse.flatten)
sealed trait HealthCheckResponse

case class HealthCheckSuccess(payload: JsonValue) extends HealthCheckResponse

object HealthCheckSuccess extends RestDataWrapperCompanion[JsonValue, HealthCheckSuccess] {
  implicit val schema: RestSchema[HealthCheckSuccess] = RestSchema.plain(Schema(`type` = DataType.Object))
}

case class HealthCheckFailure(payload: JsonValue) extends HealthCheckResponse

object HealthCheckFailure extends RestDataWrapperCompanion[JsonValue, HealthCheckFailure] {
  implicit val schema: RestSchema[HealthCheckFailure] = RestSchema.plain(Schema(`type` = DataType.Object))
}

object HealthCheckResponse extends RestDataCompanion[HealthCheckResponse] {
  // adds custom status codes
  implicit def asRestResp(implicit
    successAsRaw: AsRaw[HttpBody, HealthCheckSuccess],
    failureAsRaw: AsRaw[HttpBody, HealthCheckFailure]
  ): AsRaw[RestResponse, HealthCheckResponse] = {
    AsRaw.create {
      case s: HealthCheckSuccess => successAsRaw.asRaw(s).defaultResponse.recoverHttpError
      case f: HealthCheckFailure => failureAsRaw.asRaw(f).defaultResponse.copy(code = 500).recoverHttpError
    }
  }

  def flatten(s: Schema): Schema = {
    s.copy(oneOf = s.oneOf.map {
      case RefOr.Value(v) => v.properties.head._2
      case x => x
    })
  }
}

trait HealthCheckApi {
  @GET def live(): Future[HealthCheckResponse]

  @GET def ready(): Future[HealthCheckResponse]
}

object HealthCheckApi extends DefaultRestApiCompanion[HealthCheckApi]
