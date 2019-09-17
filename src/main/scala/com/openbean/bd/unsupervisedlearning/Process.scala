package com.openbean.bd.unsupervisedlearning

import com.openbean.bd.unsupervisedlearning.supporting.{Dimension, _}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{DataFrame, SparkSession}

class Process(dataReader: DataReader, resultWriter: Writer)(implicit spark: SparkSession) extends Logger {


  def getDataVectorised3D(data: DataFrame, columns: Map[Dimension, Array[String]]): Map[Dimension,DataFrame] = {

    Map(DimensionCPX -> Clustering.scale(Clustering.vectorise(data, columns(DimensionCPX))),
      DimensionUsage -> Clustering.scale(Clustering.vectorise(data, columns(DimensionUsage))),
      DimensionAll -> Clustering.scale(Clustering.vectorise(data, columns(DimensionAll)))
    )
  }

  private def doClustering(data: DataFrame, clusters: Int) = {
    val model = Clustering.doKMeans(clusters, data)

    val transformed = model.transform(data)

    (model, transformed)
  }


   def doClustering3D(data: Map[Dimension, DataFrame],clusters: Int,clustersAll: Int): Map[Dimension, (DataFrame, KMeansModel)] = {

    val (cpxClusterModel, cpxClusteredData) = doClustering(data(DimensionCPX), clusters)
    val (usageKPIModel, usageKPIClusteredData) = doClustering(data(DimensionUsage), clusters)
    val (model, cluster) = doClustering(data(DimensionAll), clustersAll)

    Map(DimensionCPX -> (cpxClusteredData, cpxClusterModel),
      DimensionUsage -> (usageKPIClusteredData, usageKPIModel),
      DimensionAll -> (cluster, model)
      )
  }

  def joinWithClusters(dataRaw: DataFrame,clusters : Map[Dimension, (DataFrame, KMeansModel)]): DataFrame = {
    def join(dimension: DataFrame, rawData: DataFrame, dimensionName: Dimension): DataFrame = {
      val data  =
        dimension
          .select("user_id", "prediction")
        .withColumnRenamed("prediction", dimensionName.clusteringColumnName)
      rawData.join(data, "user_id")
    }

    clusters.foldLeft(dataRaw)((data,dim) => join(dim._2._1, data, dim._1))
  }


  def run() = {
    val columnsForClustering = Map(DimensionAll -> (CXKPIsModel.getModelCols ++ UsageKPIsModel.getModelCols),
      DimensionCPX ->CXKPIsModel.getModelCols,
      DimensionUsage -> UsageKPIsModel.getModelCols)

    val allFields = columnsForClustering(DimensionAll) ++ ContractKPIsModel.getModelCols ++ CorrelatedColumns.getRemovedCols

    val dataRaw = dataReader
      .readData()
      .select("user_id", allFields: _*)

    resultWriter.writeSummaryRaw(dataRaw)

    val data = Clustering.doLog(allFields, dataRaw)


    lazy val vectorisedDimensions = getDataVectorised3D(data, columnsForClustering)

    val clusters = doClustering3D(vectorisedDimensions, 10, 20)

    val result = joinWithClusters(dataRaw, clusters)

    resultWriter.writeClusterData(clusters, result)

    resultWriter.writeCrossDimensionStats(result, Array(DimensionCPX, DimensionUsage))

  }

}