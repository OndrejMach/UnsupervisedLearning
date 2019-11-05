package com.openbean.bd.unsupervisedlearning

import com.openbean.bd.unsupervisedlearning.supporting.Logger
import org.apache.spark.ml.clustering.KMeansModel

class ModelPersistenceWriter(modelAllPath: String, modelCPXPath: String, modelUsagePath: String)  extends  Logger{
  def writeModels(modelAll: KMeansModel, modelCPX: KMeansModel, modelUsage: KMeansModel) = {
    logger.info(s"Writing all-features cluster to ${modelAllPath}")
    modelAll.write.overwrite().save(modelAllPath)
    logger.info(s"Writing CPX cluster to ${modelCPXPath}")
    modelCPX.write.overwrite().save(modelCPXPath)
    logger.info(s"Writing Usage cluster to ${modelUsagePath}")
    modelUsage.write.overwrite().save(modelUsagePath)
    logger.info("Models saved OK")
  }
}
