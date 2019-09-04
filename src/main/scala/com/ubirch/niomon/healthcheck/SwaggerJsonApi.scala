package com.ubirch.niomon.healthcheck

import io.udash.rest.openapi.OpenApi
import io.udash.rest.{DefaultRestImplicits, GET, RestApiCompanion}

import scala.concurrent.Future

trait SwaggerJsonApi {
  @GET("")
  def json: Future[OpenApi]
}

object SwaggerJsonApi extends RestApiCompanion[DefaultRestImplicits, SwaggerJsonApi](DefaultRestImplicits) {
  class Impl(openApi: OpenApi) extends SwaggerJsonApi {
    override def json: Future[OpenApi] = Future.successful(openApi)
  }
}
