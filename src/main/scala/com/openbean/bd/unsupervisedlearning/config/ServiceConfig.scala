package com.openbean.bd.unsupervisedlearning.config

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Properties

class ServiceConfig(filename: Option[String] = None) {
  val ARRAY_DELIMITER: String = ","

  val config: Config = filename.fold(ifEmpty = ConfigFactory.load())(file => ConfigFactory.load(file))

  def envOrElseConfig(name: String): String = {
    config.resolve()
    Properties.envOrElse(
      name.toUpperCase.replaceAll("""\.""", "_"),
      config.getString(name)
    )
  }
}