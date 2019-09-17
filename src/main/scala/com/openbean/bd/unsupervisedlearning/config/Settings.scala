package com.openbean.bd.unsupervisedlearning.config

import com.openbean.bd.unsupervisedlearning.supporting.Logger

case class Settings(sparkAppName: Option[String]
                    , sparkMaster: Option[String]
                    , inputDataLocation: Option[String]
                    , rawSummaryFile: Option[String]
                    , clusterStatsFile: Option[String]
                    , crossDimensionalStatsFile: Option[String]
                    , outputFile: Option[String]
                    , writeMode: Option[String]) {

  def isAllDefined: Boolean = {
    this.sparkAppName.isDefined && this.sparkAppName.get.nonEmpty &&
      this.sparkMaster.isDefined && this.sparkMaster.get.nonEmpty &&
      this.inputDataLocation.isDefined && this.inputDataLocation.get.nonEmpty &&
      this.rawSummaryFile.isDefined && this.rawSummaryFile.get.nonEmpty &&
      this.clusterStatsFile.isDefined && this.clusterStatsFile.get.nonEmpty &&
      this.crossDimensionalStatsFile.isDefined && this.crossDimensionalStatsFile.get.nonEmpty &&
      this.outputFile.isDefined && this.outputFile.get.nonEmpty &&
      this.writeMode.isDefined && this.writeMode.get.nonEmpty
  }

  def listParams() = {
    val fields = this.getClass.getDeclaredFields

    val params = for (field <- fields)
      yield
        {field.setAccessible(true); s"${Console.RED}${Console.BOLD}Parameter ${field.getName}:${Console.RESET} ${field.get(this).asInstanceOf[Option[String]].getOrElse("N/A")}"}

    params.mkString("\n")
  }

}
