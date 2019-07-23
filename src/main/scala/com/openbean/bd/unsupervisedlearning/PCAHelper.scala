package com.openbean.bd.unsupervisedlearning

import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.DataFrame

object PCAHelper extends Logger {

  def getPCA(k: Int, data: DataFrame) = {
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(k)
      .fit(data)


    //println(pca.explainParams())

    println(pca.explainedVariance)

    //println(pca.pc)

    pca.pc.rowIter.foreach(println(_))

    //val pw = new PrintWriter(new File("/Users/ondrej.machacek/tmp/PC.csv"))

    // pw.write(matrix.toString(10,10))

    //pw.close()

    val result = pca.transform(data)
    result.select("pcaFeatures").show(false)

    result.drop("features").withColumnRenamed("pcaFeatures", "features")
  }


}