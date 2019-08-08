package com.openbean.bd.unsupervisedlearning

import java.io._

import com.openbean.bd.unsupervisedlearning.supporting.{CXKPIsColumns, CXKPIsModel, ContractKPIsColumns, ContractKPIsModel, Dimension, DimensionCPX, DimensionContract, DimensionUsage, Logger, UsageKPIsColumns, UsageKPIsModel}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml._


case class Stat(mean: Double, stddev: Double)

class Process(dataReader: DataReader)(implicit spark: SparkSession) extends Logger {


  def getDataVectorised3D(data: DataFrame): (DataFrame, DataFrame) = {
    (Clustering.vectorise(data, CXKPIsModel.getModelCols),
      // Clustering.vectorise(data,ContractKPIsModel.getModelCols),
      Clustering.vectorise(data, UsageKPIsModel.getModelCols))
  }

  private def doClustering(data: DataFrame, clusters: Int) = {
    val model = Clustering.doKMeans(clusters, data)

    val transformed = model.transform(data)

    (model, transformed)
  }


  private def doClustering3D(cpxData: DataFrame, usageData: DataFrame, clusters: Int): Map[Dimension, (DataFrame, KMeansModel)] = {

    val (cpxClusterModel, cpxClusteredData) = doClustering(cpxData, clusters)
    //val (contractKPIModel,usageKPIClusteredData) = doClustering(contractData,clusters)
    val (usageKPIModel, usageKPIClusteredData) = doClustering(usageData, clusters)

    Map(DimensionCPX -> (cpxClusteredData, cpxClusterModel),
      // DimensionContract -> (contractKPIClusteredData, contractKPIModel),
      DimensionUsage -> (usageKPIClusteredData, usageKPIModel))
  }


  def do3D(cpxDataVectorised: DataFrame, usageKPIDataVectorised: DataFrame,
           clusters: Int, //outputFilenameStats: String, outputFilenameData: String,
           cpxCols: Array[String] = CXKPIsModel.getModelCols,
           usageCols: Array[String] = UsageKPIsModel.getModelCols)
          (implicit spark: SparkSession) = {


    //val dataClustered = doClustering3D(cpxDataVectorised,contractKPIVectorised,usageKPIDataVectorised,clusters   )
    doClustering3D(cpxDataVectorised, usageKPIDataVectorised, clusters)

  }

  def doAll(data: DataFrame, clusters: Int) = {
    //option("header","true").mode(SaveMode.Overwrite).

    doClustering(data, clusters)

    //ResultWriter.writeClusterStats(transformed,model,allFields,filename)
  }

  def correctOutliers(fields: Array[String], data: DataFrame): DataFrame = {

    import spark.implicits._

    var tmp = data
    logger.info(s"With Outliers ${tmp.count()}")


    for (i <- fields) {
      logger.info(s"Outlier removal for ${i} count: ${tmp.count()}")
      val stats = tmp.select(mean(i).alias("mean"), stddev_pop(i).alias("stddev")).as[Stat].collect()(0)
      val min = stats.mean - 50 * stats.stddev
      val max = stats.mean + 50 + stats.stddev
      logger.info(s"${stats}")
      tmp = tmp.withColumn(i, when(col(i).gt(lit(max)), lit(max)).otherwise(col(i)))
      logger.info(s"Without outliers for ${i} count: ${tmp.count()}")
    }
    logger.info(s"After Outliers removal count ${tmp.count()}")
    tmp
  }

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

  def writeSummaryRaw(data: DataFrame) = {
    data.summary().repartition(1).write.option("header", "true").csv("/Users/ondrej.machacek/tmp/AllDesc.csv")
  }

  def writeSummaryScaled(data: DataFrame, fields: Array[String]) = {
    val vecToArray = udf((xs: linalg.Vector) => xs.toArray)
    val dfArr = data.withColumn("featuresArr", vecToArray(col("features")))

    val sqlExpr = fields.zipWithIndex.map { case (alias, idx) => col("featuresArr").getItem(idx).as(alias) }

    val scaledDF = dfArr.select(sqlExpr: _*)

    scaledDF.summary().repartition(1).write.option("header", "true").csv("/Users/ondrej.machacek/tmp/AllDescScaled.csv")
  }




  def run() = {
    val allFields = CXKPIsModel.getModelCols ++ UsageKPIsModel.getModelCols ++ ContractKPIsModel.getModelCols

    //println(s"${allFields(0)}:::${allFields.tail.mkString(",")}")

    //allFields.foreach(println(_))

    val dataRaw = dataReader
      .readData("/Users/ondrej.machacek/Projects/TMobile/data/unsupervised/part-00000-892b3c0f-ad5f-4ab9-aaa9-3c668a3be26d-c000.snappy.parquet")
      .select("user_id", allFields: _*)

    //dataRaw.printSchema()

    val data = doLog(allFields, dataRaw)


    lazy val (cpxDataVectorised, usageKPIDataVectorised) = getDataVectorised3D(data)

    //lazy val allVectorised = Clustering.vectorise(data, allFields)

    Clustering.printCostAnalysis(5,30,cpxDataVectorised)
    Clustering.printCostAnalysis(lower = 5, upper = 30, usageKPIDataVectorised)



    val clusters = do3D(cpxDataVectorised, usageKPIDataVectorised, 20)
    //println(data.select("cex_tel_per_call_avg").filter("cex_tel_per_call_avg is null").count())

    ResultWriter.writeClusterStats3D(clusters, CXKPIsModel.getModelCols, UsageKPIsModel.getModelCols, "/Users/ondrej.machacek/tmp/ClusterStats.csv")

    ResultWriter.writeClusterData(clusters, dataRaw, "LogOnly")

    val outliersCorrected = correctOutliers(allFields, data)

    val (cpxCorrected, usageCorrected) = getDataVectorised3D(outliersCorrected)

    val clustersOutliers = do3D(cpxCorrected, usageCorrected, 20)
    ResultWriter.writeClusterStats3D(clustersOutliers, CXKPIsModel.getModelCols, UsageKPIsModel.getModelCols, "/Users/ondrej.machacek/tmp/ClusterStatsWithoutOutliers.csv")

    ResultWriter.writeClusterData(clusters, dataRaw, "LogOutliersCorrected")

    Clustering.printCostAnalysis(5,30,cpxCorrected)
    Clustering.printCostAnalysis(lower = 5, upper = 30, usageCorrected)


    //val scaled = Clustering.scale(allVectorised).select("features")

    //scaled.printSchema()


    //scaledDF.show(truncate = false)


  }

}