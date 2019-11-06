package com.openbean.bd.unsupervisedlearning

import com.openbean.bd.unsupervisedlearning.supporting.{DimensionAll, DimensionCPX, DimensionUsage, Logger}
import org.apache.spark.ml.clustering.KMeansModel

class ModelPersistenceWriter(modelPersistence: String)  extends  Logger{
  def writeModels(modelAll: KMeansModel, modelCPX: KMeansModel, modelUsage: KMeansModel) = {
    logger.info(s"Writing all-features cluster to ${modelPersistence}/${DimensionAll.name}")
    modelAll.write.overwrite().save(s"${modelPersistence}/${DimensionAll.name}")
    logger.info(s"Writing CPX cluster to ${modelPersistence}/${DimensionCPX.name}")
    modelCPX.write.overwrite().save(s"${modelPersistence}/${DimensionCPX.name}")
    logger.info(s"Writing Usage cluster to ${modelPersistence}/${DimensionUsage.name}")
    modelUsage.write.overwrite().save(s"${modelPersistence}/${DimensionUsage.name}")
    logger.info("Models saved OK")
  }
}
