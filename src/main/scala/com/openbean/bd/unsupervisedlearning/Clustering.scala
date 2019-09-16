package com.openbean.bd.unsupervisedlearning

import com.openbean.bd.unsupervisedlearning.supporting.{CXKPIsColumns, Logger, UsageKPIsColumns}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, col, log}

case class ClusterStats(id: Int,clusterCenter: Vector, count: Long )


object Clustering extends Logger {
  def doLog(fields: Array[String], data: DataFrame): DataFrame = {

    var tmp = data
    logger.info(s"Transforming to Logarithm")

    for (i <- fields if (!i.equals(CXKPIsColumns.avg_thp_dl_mbps.toString) && !i.equals(UsageKPIsColumns.lte_ratio.toString))) {
      logger.info(s"Log transformation for ${i}")
      tmp = tmp.withColumn(i, log(col(i) + 1))
    }
    logger.info(s"Log transformation DONE ${tmp.count()}")
    tmp
  }

  def vectorise(input: DataFrame, valueColumns: Array[String]): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(valueColumns)
      .setOutputCol("features")

    val corrected = input.na.fill(0,valueColumns)

    assembler.transform(corrected)//.drop("user_id")
  }

  def oneHot(input: DataFrame, column: String) : DataFrame = {
    val tmpCol = "tmp"

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array(column))
      .setOutputCols(Array("tmp"))
    val model = encoder.fit(input)

    val encoded = model.transform(input)

    encoded.drop(column).withColumnRenamed(tmpCol,column)
  }

  def doKMeans(clusters: Int, dataForClustering: DataFrame ) : KMeansModel = {

    val kmeans = new KMeans().setK(clusters).setSeed(1L)
    val model = kmeans.fit(dataForClustering)

    val WSSSE = model.computeCost(dataForClustering)
    println(WSSSE)
    model

  }

  def printCostAnalysis(lower: Int, upper: Int, data: DataFrame) : Unit = {

    logger.info("Starting cost analysis")
    for {i <- lower to upper } {
      doKMeans(i, data)
    }
    logger.info("Cost analysis DONE")
  }


  def getClusterStats(clusteredData : DataFrame /*, clusterCenters: Array[Vector]*/) : DataFrame = {
    //case class Stats2(prediction: Int, count: Long)
    //withPrediction.printSchema()

    //import spark.implicits._

    clusteredData
      .groupBy("prediction")
      .count().alias("count")
      .orderBy(asc("prediction"))
      //.drop("prediction")
      //.as[Long]
      //.collect()
    //for {i <- 0 to (clusterCenters.length-1)} yield {new ClusterStats(i,clusterCenters(i), grouped(i))}
  }

  def scale(data: DataFrame): DataFrame = {
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(true)

    val scalerModel = scaler.fit(data)
    scalerModel.transform(data).drop("features").withColumnRenamed("scaledFeatures","features")
  }

}