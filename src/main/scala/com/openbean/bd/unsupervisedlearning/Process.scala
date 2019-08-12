package com.openbean.bd.unsupervisedlearning

import java.io._

import com.openbean.bd.unsupervisedlearning.supporting.{CXKPIsColumns, CXKPIsModel, ContractKPIsColumns, ContractKPIsModel, Dimension, DimensionAll, DimensionCPX, DimensionContract, DimensionUsage, Logger, UsageKPIsColumns, UsageKPIsModel}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml._


case class Stat(mean: Double, stddev: Double)

class Process(dataReader: DataReader, resultWriter: ResultWriter)(implicit spark: SparkSession) extends Logger {


  def getDataVectorised3D(data: DataFrame): (DataFrame, DataFrame, DataFrame) = {
    (Clustering.vectorise(data, CXKPIsModel.getModelCols),
      Clustering.vectorise(data,ContractKPIsModel.getModelCols),
      Clustering.vectorise(data, UsageKPIsModel.getModelCols))
  }

  private def doClustering(data: DataFrame, clusters: Int) = {
    val model = Clustering.doKMeans(clusters, data)

    val transformed = model.transform(data)

    (model, transformed)
  }


  private def doClustering3D(cpxData: DataFrame, usageData: DataFrame, contractData: DataFrame,clusters: Int): Map[Dimension, (DataFrame, KMeansModel)] = {

    val (cpxClusterModel, cpxClusteredData) = doClustering(cpxData, clusters)
    val (contractKPIModel,contractKPIClusteredData) = doClustering(contractData,clusters)
    val (usageKPIModel, usageKPIClusteredData) = doClustering(usageData, clusters)

    Map(DimensionCPX -> (cpxClusteredData, cpxClusterModel),
       DimensionContract -> (contractKPIClusteredData, contractKPIModel),
      DimensionUsage -> (usageKPIClusteredData, usageKPIModel))
  }

  def doAllClustering(data: DataFrame, clusters: Int) : Map[Dimension, (DataFrame, KMeansModel)] = {
    val (model,cluster) = doClustering(data, clusters)
    Map(DimensionAll -> (cluster, model) )
  }

  def do3D(cpxDataVectorised: DataFrame, usageKPIDataVectorised: DataFrame, contractKPIVectorised: DataFrame, clusters: Int) (implicit spark: SparkSession) = {


    doClustering3D(cpxDataVectorised,contractKPIVectorised,usageKPIDataVectorised,clusters   )
    //doClustering3D(cpxDataVectorised, usageKPIDataVectorised, clusters)

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


  def run() = {
    val allFields = CXKPIsModel.getModelCols ++ UsageKPIsModel.getModelCols ++ ContractKPIsModel.getModelCols

    val dataRaw = dataReader
      .readData()
      .select("user_id", allFields: _*)

    val data = doLog(allFields, dataRaw)


    lazy val (cpxDataVectorised,contractKPIsVectorised, usageKPIDataVectorised) = getDataVectorised3D(data)

    val allFieldsVectorised =Clustering.vectorise(data,allFields)

    val clusters = do3D(cpxDataVectorised, usageKPIDataVectorised, contractKPIsVectorised,10)

    resultWriter.writeClusterData(clusters, dataRaw)

    resultWriter.writeCrossDimensionStats(clusters(DimensionCPX)._1, clusters(DimensionUsage)._1, clusters(DimensionContract)._1)

    val clusterAll = doAllClustering(allFieldsVectorised, 20)
    resultWriter.writeClusterData(clusterAll,dataRaw)

  }

}