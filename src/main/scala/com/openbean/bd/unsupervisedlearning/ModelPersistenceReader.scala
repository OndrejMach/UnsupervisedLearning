package com.openbean.bd.unsupervisedlearning

import com.openbean.bd.unsupervisedlearning.supporting.{Dimension, DimensionAll, DimensionCPX, DimensionUsage, Logger}
import org.apache.spark.ml.clustering.KMeansModel

class ModelPersistenceReader(modelPersistence: String)  extends  Logger{
  def loadModels(): Map[Dimension, KMeansModel] = {
    logger.info(s"Loading all-features cluster from ${modelPersistence}/${DimensionAll.name}")
     val modelAll = KMeansModel.load(s"${modelPersistence}/${DimensionAll.name}")
    logger.info(s"Loading CPX cluster from ${modelPersistence}/${DimensionCPX.name}")
    val modelCPX = KMeansModel.load(s"${modelPersistence}/${DimensionCPX.name}")
    logger.info(s"Loading Usage cluster from ${modelPersistence}/${DimensionUsage.name}")
    val modelUsage = KMeansModel.load(s"${modelPersistence}/${DimensionUsage.name}")
    logger.info("Models loaded OK")

    Map(DimensionAll-> modelAll, DimensionCPX -> modelCPX, DimensionUsage -> modelUsage)
  }

}
