package com.openbean.bd.unsupervisedlearning

import com.openbean.bd.unsupervisedlearning.supporting.{Dimension, _}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProcessTraining(dataReader: DataReader, resultWriter: Writer, modelPersistenceWriter: ModelPersistenceWriter)(implicit spark: SparkSession) extends Processor(dataReader) {


  private def doClustering(data: DataFrame, clusters: Int) : KMeansModel = {
    logger.info(s"Doing clustering with clusters #${clusters}")
    val model = Clustering.doKMeans(clusters, data)

    //val transformed = model.transform(data)

    model //, transformed)
  }

/*
  def doClustering3D(data: Map[Dimension, DataFrame], clusters: Int, clustersAll: Int): Map[Dimension, (DataFrame, KMeansModel)] = {
    logger.info(s"Doing clustering for dimension ${DimensionCPX.name}")
    val (cpxClusterModel, cpxClusteredData) = doClustering(data(DimensionCPX), clusters)
    logger.info(s"Doing clustering for dimension ${DimensionUsage.name}")
    val (usageKPIModel, usageKPIClusteredData) = doClustering(data(DimensionUsage), clusters)
    logger.info(s"Doing clustering for dimension ${DimensionAll.name}")
    val (model, cluster) = doClustering(data(DimensionAll), clustersAll)

    Map(DimensionCPX -> (cpxClusteredData, cpxClusterModel),
      DimensionUsage -> (usageKPIClusteredData, usageKPIModel),
      DimensionAll -> (cluster, model)
    )
  }

 */

  def doClustering3D(data: Map[Dimension, DataFrame], clusters: Int, clustersAll: Int): Map[Dimension, KMeansModel] = {
    logger.info(s"Doing clustering for dimension ${DimensionCPX.name}")
    val cpxClusterModel = doClustering(data(DimensionCPX), clusters)
    logger.info(s"Doing clustering for dimension ${DimensionUsage.name}")
    val usageKPIModel = doClustering(data(DimensionUsage), clusters)
    logger.info(s"Doing clustering for dimension ${DimensionAll.name}")
    val model = doClustering(data(DimensionAll), clustersAll)

    Map(DimensionCPX -> cpxClusterModel,
      DimensionUsage -> usageKPIModel,
      DimensionAll -> model
    )
  }
  def joinWithClusters(dataRaw: DataFrame, clusters: Map[Dimension, (DataFrame, KMeansModel)]): DataFrame = {
    def join(dimension: DataFrame, rawData: DataFrame, dimensionName: Dimension): DataFrame = {
      val data =
        dimension
          .select("user_id", "prediction")
          .withColumnRenamed("prediction", dimensionName.clusteringColumnName)
      rawData.join(data, "user_id")
    }

    clusters.foldLeft(dataRaw)((data, dim) => join(dim._2._1, data, dim._1))
  }


  override def run(): Unit = {

    resultWriter.writeSummaryRaw(dataRaw)
    logger.info("Transforming data using Log function")
    val data = Clustering.doLog(allFields, dataRaw)

    logger.info("Vectorising all the dimensions")
    lazy val vectorisedDimensions = Clustering.getDataVectorised3D(data, columnsForClustering)
    logger.info("Clustering started")
    val clusters = doClustering3D(vectorisedDimensions, 10, 20)
    logger.info("Preparing for storing cluster's configuration")
    modelPersistenceWriter.writeModels(clusters(DimensionAll), clusters(DimensionCPX), clusters(DimensionUsage))

  }

}