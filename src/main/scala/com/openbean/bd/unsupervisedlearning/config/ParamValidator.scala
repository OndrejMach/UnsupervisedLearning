package com.openbean.bd.unsupervisedlearning.config

import com.openbean.bd.unsupervisedlearning.supporting.Logger
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

object ParamValidator extends Logger{
  private def checkInputPathExit(path: String)(implicit sparkSession: SparkSession) : Boolean = {
    val hdfsConf = sparkSession.sparkContext.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hdfsConf)
    val hdfsPath = new org.apache.hadoop.fs.Path((path))
    logger.info(s"Checking folder ${path} exists")
    if (!pathExists(path)) {
      logger.error(s"Input folder does not exist, no data to process.. ${path}")
      return false
    }
    logger.info(s"Checking folder ${path} for data")
    if (!hdfs.listFiles(hdfsPath, true).hasNext) {
      logger.error(s"Input folder is empty, no data to process.. ${path}")
      return false
    }
    true
  }

  private def pathExists(path: String)(implicit sparkSession: SparkSession): Boolean = {
    logger.info(s"Checking ${path}")
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.exists(new Path(path))
  }

  def validateSettings(params: Settings)(implicit sparkSession: SparkSession) : Array[String] = {
    val invalidParams = Array[String]()

    if (!(params.writeMode.get match {
      case "excel" => true
      case "parquet" => true
      case _ => false
    })) invalidParams ++ Array("writeMode")

    if (!checkInputPathExit(params.inputDataLocation.get)) invalidParams ++ Array("InputDataLocation")

    invalidParams

  }
}
