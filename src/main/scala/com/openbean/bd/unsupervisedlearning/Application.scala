package com.openbean.bd.unsupervisedlearning

import com.openbean.bd.unsupervisedlearning.config.{ParamValidator, Setup}
import com.openbean.bd.unsupervisedlearning.supporting.Logger
import org.apache.spark.sql.SparkSession

object Application extends App with Logger {
  val setup = new Setup()

  logger.info(setup.banner)
  logger.info(s"Application parameters:\n${setup.settings.listParams}")

  logger.info("Creating Spark session")

  implicit val spark = SparkSession.builder()
    //.appName(setup.settings.sparkAppName.get)
    .master(setup.settings.sparkMaster.get)
    .config("spark.app.name", setup.settings.sparkAppName.get)
    .config("spark.executor.memory", "6g")
    .config("spark.driver.memory", "10g")
    .config("spark.driver.maxResultSize", "10g")
    .config("spark.executor.JavaOptions", "-XX:+UseG1GC")
    .config("spark.executor.extraJavaOptions", "-XX:InitiatingHeapOccupancyPercent=35")
    .config("spark.dynamicAllocation.enabled", "true")
    .getOrCreate()

  logger.info("Validating parameters")
  val invalidParams = ParamValidator.validateSettings(setup.settings)
  if (!invalidParams.isEmpty) {
    logger.error(s"Parameter(s) ${invalidParams.mkString(",")} not properly specified")
    System.exit(1)
  }
  logger.info("Reading input data")
  val reader = new DataReaderParquet(setup.settings.inputDataLocation.get)
  logger.info("Preparing writer")
  val writer = new ResultWriter(setup.settings.crossDimensionalStatsFile.get, setup.settings.rawSummaryFile.get, setup.settings.clusterStatsFile.get, setup.settings.outputFile.get, setup.settings.writeMode.get)


  val processor = args(0) match {
    case "training" => {
      logger.info("preparing model persistence functionality")
      val modelPersistenceWriter = new ModelPersistenceWriter(setup.settings.modelPersistence.get)
      logger.info("creating data processor")
      new ProcessTraining(reader, writer, modelPersistenceWriter)
    }
    case "transform" => {
      logger.info("preparing model persistence functionality")
      val modelPersistenceReader = new ModelPersistenceReader(setup.settings.modelPersistence.get)
      logger.info("creating data processor")
      new ProcessTransformation(reader, writer, modelPersistenceReader, setup.settings.sampleRate.get)
    }
  }


  logger.info("Processing started")
  processor.run()
  logger.info("Processing finished successfully")
}
