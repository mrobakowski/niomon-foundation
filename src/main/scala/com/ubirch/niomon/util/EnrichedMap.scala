package com.ubirch.niomon.util

import scala.language.implicitConversions

class EnrichedMap(map: Map[String, String]) {

  object CaseInsensitive {

    def get(key: String): Option[String] = map
      .find{ case (k, _) => k.toLowerCase == key.toLowerCase }
      .map { case (_, v) => v}

    def getOrElse(key: String, default: => String): String = get(key) match {
      case Some(v) => v
      case None => default
    }

    def contains(key: String): Boolean = get(key).isDefined

  }

}

object EnrichedMap {
  implicit def toEnrichedMap(map: Map[String, String]): EnrichedMap = new EnrichedMap(map)
}
