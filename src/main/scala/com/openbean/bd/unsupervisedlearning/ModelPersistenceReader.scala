package com.openbean.bd.unsupervisedlearning

import com.openbean.bd.unsupervisedlearning.supporting.{Dimension, DimensionAll, DimensionCPX, DimensionUsage, Logger}
import org.apache.spark.ml.clustering.KMeansModel

class ModelPersistenceReader(modelAllPath: String, modelCPXPath: String, modelUsagePath: String)  extends  Logger{
  def loadModels(): Map[Dimension, KMeansModel] = {
    logger.info(s"Loading all-features cluster from ${modelAllPath}")
     val modelAll = KMeansModel.load(modelAllPath)
    logger.info(s"Loading CPX cluster from ${modelCPXPath}")
    val modelCPX = KMeansModel.load(modelCPXPath)
    logger.info(s"Loading Usage cluster from ${modelUsagePath}")
    val modelUsage = KMeansModel.load(modelUsagePath)
    logger.info("Models loaded OK")

    Map(DimensionAll-> modelAll, DimensionCPX -> modelCPX, DimensionUsage -> modelUsage)
  }

}
