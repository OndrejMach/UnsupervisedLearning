package com.openbean.bd.unsupervisedlearning

import com.openbean.bd.unsupervisedlearning.supporting.Logger
import org.apache.spark.sql.SparkSession

object Application extends App with Logger {
  val inputPath = "/Users/ondrej.machacek/Projects/TMobile/data/unsupervised/flowgraph_20190205-0000_20190211-0000__cust_exp_weekly_aggregations"



  logger.info("Creating Spark session")

  implicit val spark = SparkSession.builder()
    .appName("Unsupervised learning")
    .master("local[*]")
    .config("spark.executor.memory", "6g")
    .config("spark.driver.memory", "10g")
    .config("spark.driver.maxResultSize", "10g")
    .config("spark.executor.JavaOptions","-XX:+UseG1GC" )
    .config("spark.executor.extraJavaOptions", "-XX:InitiatingHeapOccupancyPercent=35")
    .config("spark.dynamicAllocation.enabled", "true")
    .getOrCreate()


  logger.info("Reading data")
  val reader = new DataReader()
  val data = reader.readData(inputPath)
  data.printSchema()

}
