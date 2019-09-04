package com.ubirch.niomon.healthcheck

import com.typesafe.scalalogging.StrictLogging
import io.udash.rest.RestServlet
import io.udash.rest.openapi.OpenApi
import org.eclipse.jetty
import org.eclipse.jetty.servlet.{DefaultServlet, ServletContextHandler, ServletHolder}

class JettyServer(api: HealthCheckApi, docs: OpenApi, port: Int) extends StrictLogging {
  private val healthCheckApiServlet = RestServlet[HealthCheckApi](api)
  private val server = new jetty.server.Server(port)
  private val servletHandler = new ServletContextHandler

  val swaggerPath = "/swagger" // if this is changed, `resources/swagger/index.html` also has to be tweaked

  def start(): Unit = {
    servletHandler.addServlet(new ServletHolder(healthCheckApiServlet), "/health/*")
    addSwagger(docs, swaggerPath)

    server.setHandler(servletHandler)
    server.start()
  }

  def join(): Unit = server.join()

  private def addSwagger(openApi: OpenApi, swaggerPrefix: String): Unit = {
    // add swagger static files
    val swaggerResourceUrl = getClass.getClassLoader.getResource("swagger/index.html").toString.stripSuffix("index.html")

    logger.debug(s"swagger root url: $swaggerResourceUrl")

    val swaggerStaticServletHolder = new ServletHolder("swagger", classOf[DefaultServlet])
    swaggerStaticServletHolder.setInitParameter("pathInfoOnly", "true")
    swaggerStaticServletHolder.setInitParameter("resourceBase", swaggerResourceUrl)

    servletHandler.addServlet(swaggerStaticServletHolder, s"$swaggerPrefix/*")

    // add the dynamically generated swagger.json
    val swaggerJsonServlet = RestServlet[SwaggerJsonApi](new SwaggerJsonApi.Impl(openApi))
    servletHandler.addServlet(new ServletHolder(swaggerJsonServlet), s"$swaggerPrefix/swagger.json")
  }
}
