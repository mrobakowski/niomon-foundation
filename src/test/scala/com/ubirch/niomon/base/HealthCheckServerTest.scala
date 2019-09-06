package com.ubirch.niomon.base

import com.ubirch.niomon.healthcheck.{CheckResult, HealthCheckServer}
import org.json4s.JsonAST.JObject
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Future

class HealthCheckServerTest extends FlatSpec with Matchers {
  "HealthCheckServer" should "work" ignore {
    val server = new HealthCheckServer(
      livenessChecks = Map("foo" -> { () => Future.successful(CheckResult("foo", success = true, JObject())) }),
      readinessChecks = Map("bar" -> { () => Future.successful(CheckResult("bar", success = false, JObject())) }),
      "http://localhost:8888/health"
    )
    server.run(8888)
    server.join()
  }
}
