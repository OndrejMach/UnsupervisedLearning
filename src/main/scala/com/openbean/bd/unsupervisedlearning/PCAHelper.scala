package com.openbean.bd.unsupervisedlearning

import com.openbean.bd.unsupervisedlearning.supporting.Logger
import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.DataFrame

object PCAHelper extends Logger {

  def getPCA(k: Int, data: DataFrame) = {
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(k)
      .fit(data)

    val result = pca.transform(data)

    (result.drop("features").withColumnRenamed("pcaFeatures", "features"),pca.explainedVariance, pca.pc)
  }


}