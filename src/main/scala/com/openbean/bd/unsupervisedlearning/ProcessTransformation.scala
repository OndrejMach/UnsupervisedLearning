package com.openbean.bd.unsupervisedlearning

import com.openbean.bd.unsupervisedlearning.supporting.{Dimension, _}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProcessTransformation(dataReader: DataReader, resultWriter: Writer, modelPersistenceReader: ModelPersistenceReader, sampleRate: Double)(implicit spark: SparkSession) extends Processor(dataReader) {


  private def doTransformation(data: DataFrame, cluster: KMeansModel) = {

    logger.info(s"Transforming data ")
    //val model = Clustering.doKMeans(clusters, data)

    cluster.transform(data)

    //model //, transformed)
  }


  def doClustering3D(data: Map[Dimension, DataFrame], clusters: Map[Dimension, KMeansModel]): Map[Dimension, DataFrame] = {
    logger.info(s"Doing Transformation for dimension ${DimensionCPX.name}")
    val cpxClusteredData = doTransformation(data(DimensionCPX), clusters(DimensionCPX))
    logger.info(s"Doing Transformation for dimension ${DimensionUsage.name}")
    val usageKPIClusteredData = doTransformation(data(DimensionUsage), clusters(DimensionUsage))
    logger.info(s"Doing Transformation for dimension ${DimensionAll.name}")
    val  cluster = doTransformation(data(DimensionAll), clusters(DimensionAll))

    Map(DimensionCPX -> cpxClusteredData,
      DimensionUsage -> usageKPIClusteredData,
      DimensionAll -> cluster
    )
  }

  def joinWithClusters(dataRaw: DataFrame, clusters: Map[Dimension, DataFrame]): DataFrame = {
    def join(dimension: DataFrame, rawData: DataFrame, dimensionName: Dimension): DataFrame = {
      val data =
        dimension
          .select("user_id", "prediction")
          .withColumnRenamed("prediction", dimensionName.clusteringColumnName)
      rawData.join(data, "user_id")
    }

    clusters.foldLeft(dataRaw)((data, dim) => join(dim._2, data, dim._1))
  }


  override def run() = {

    resultWriter.writeSummaryRaw(dataRaw)
    logger.info("Transforming data using Log function")
    val data = Clustering.doLog(allFields, dataRaw)

    logger.info("Vectorising all the dimensions")
    lazy val vectorisedDimensions = Clustering.getDataVectorised3D(data, columnsForClustering)
    logger.info("loading models")
    val models = modelPersistenceReader.loadModels()

    val clusters = doClustering3D(vectorisedDimensions, models)

    //logger.info("Preparing for storing cluster's configuration")
    //modelPersistenceWriter.writeModels(clusters(DimensionAll), clusters(DimensionCPX), clusters(DimensionUsage))

    logger.info("Joining cluster data with original data")
    val result = joinWithClusters(dataRaw, clusters)
    logger.info("Writing cluster data")
    resultWriter.writeClusterData(clusters, result)
    logger.info("Writing cross dimensional stats")
    resultWriter.writeCrossDimensionStats(result, Array(DimensionCPX, DimensionUsage))
    logger.info("Adding cluster info to the original data")
    resultWriter.writeResult(result,)
  }

}