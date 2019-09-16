package com.openbean.bd.unsupervisedlearning

import java.io._

import com.openbean.bd.unsupervisedlearning.supporting._
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

class ResultWriter(masterPath: String) extends Logger {
  def writeCrossDimensionStats(cpxData: DataFrame,
                               usageKPIData: DataFrame//,
                              // contractData: DataFrame
                               ) = {

    val toJoinU = usageKPIData
      .drop("features")
      .withColumnRenamed("prediction", "cluster_id_usage")
    val toJoinCPX = cpxData
      .drop("features")
      .withColumnRenamed("prediction", "cluster_id_cpx")
    /* val toJoinContr = contractData
      .drop("features")
      .withColumnRenamed("prediction", "cluster_id_contract")*/


    val joined = toJoinU
      .join(toJoinCPX,"user_id")
     // .join(toJoinContr, "user_id")
      .filter("user_id is not null")


    joined.printSchema()

    case class ClusterDetail(prediction_usage: Int,prediction_cpx: Int, prediction_contract: Int, count: Long)

    val grouped = joined.select("cluster_id_usage","cluster_id_cpx"/*,"cluster_id_contract" */)
      .groupBy("cluster_id_usage","cluster_id_cpx"/*,"cluster_id_contract"*/)
      .count()
    //.as[ClusterDetail]
    grouped.show(false)
    //println(grouped.count)

    grouped
      .coalesce(1)
      .write
      .format("com.crealytics.spark.excel")
      .option("sheetName",  s"Cross-cluster-stats")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d")
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
      .mode("append")
      .save("/Users/ondrej.machacek/tmp/testexcel.xlsx")
      //.write
      //.option("header","true")
      //.mode(SaveMode.Overwrite)
      //.csv(s"${masterPath}/CrossDimensionStats.csv")
  }

  def addPCAStats(explained: DenseVector, matrix: DenseMatrix, filename: String) = {
    val pw = new PrintWriter(new File(filename))
    pw.append(s"Explained variance:\n${explained}\nPC:\n${matrix}")
    pw.close()
  }

  def writeClusterData(dataClustered: Map[Dimension, (DataFrame, KMeansModel)], dataRaw: DataFrame): Unit = {

    def aggregated(dim: Dimension, array: Array[String]) : DataFrame = {
     logger.info("Joining cluster data with the original table")
      val data =  dataClustered(dim)._1
        .select("user_id", "prediction")

      val joined = data.join(dataRaw, "user_id")
        .select("prediction", array: _*)

      logger.info("Join DONE")

      joined.summary().show(false)

      logger.info("Aggregating on clusters")
      val grouped = joined
        .groupBy("prediction").mean(array: _*)

      logger.info("Getting counts")
      val counts = Clustering.getClusterStats(data)

      logger.info("Joining with counts")
      grouped.join(counts, "prediction").sort("prediction")
    }

    logger.info("Starting preparation for writing cluster data")
    for {i <- dataClustered.keys} {
      logger.info(s"Data calculation for dimension ${i.name}")
      val result = i match {
        case DimensionCPX => aggregated(DimensionCPX, CXKPIsModel.getModelCols ++ ContractKPIsModel.getModelCols ++ CXCorrelatedColumns.getRemovedCols)
        case DimensionUsage => aggregated(DimensionUsage, UsageKPIsModel.getModelCols ++ ContractKPIsModel.getModelCols++ UsageCorrelatedColumns.getRemovedCols)
        case DimensionContract => aggregated(DimensionContract, ContractKPIsModel.getModelCols)
        case DimensionAll => aggregated(DimensionAll, CXKPIsModel.getModelCols ++ UsageKPIsModel.getModelCols ++ ContractKPIsModel.getModelCols++ CorrelatedColumns.getRemovedCols)
      }
      logger.info("Cluster data ready to be written into a file")
      //result.summary().show(false)


      result
        .coalesce(1)
        .write
        .format("com.crealytics.spark.excel")
        .option("sheetName", s"Cluster ${i.name}")
        .option("useHeader", "true")
        .option("dateFormat", "yy-mmm-d")
        .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
        .mode("append")
        .save("/Users/ondrej.machacek/tmp/testexcel.xlsx")

      //.write
        //.option("header", "true")
        //.mode(SaveMode.Overwrite)
        //.csv(s"${masterPath}/${i.name}_ClusterMeans.csv")
      logger.info("Writing cluster data DONE")

    }
  }

  def writeSummaryRaw(data: DataFrame) = {
    data
      .summary()
      .repartition(1)
      .write
      .format("com.crealytics.spark.excel")
      .option("sheetName", "Summary All Fields")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d")
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
      .mode("append")
      .save("/Users/ondrej.machacek/tmpp/testexcel.xlsx")
  }

  def writeSummaryScaled(data: DataFrame, fields: Array[String]) = {
    val vecToArray = udf((xs: linalg.Vector) => xs.toArray)
    val dfArr = data.withColumn("featuresArr", vecToArray(col("features")))

    val sqlExpr = fields.zipWithIndex.map { case (alias, idx) => col("featuresArr").getItem(idx).as(alias) }

    val scaledDF = dfArr.select(sqlExpr: _*)

    scaledDF
      .summary()
      .repartition(1)
      .write
      .format("com.crealytics.spark.excel")
      .option("sheetName", "Summary All Fields")
      .option("useHeader", "true")
      .option("dateFormat", "yy-mmm-d")
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
      .mode("append")
      .save("/Users/ondrej.machacek/tmp/testexcel.xlsx")
    //.write.option("header", "true")
      //.csv(s"${masterPath}AllDescScaled.csv")
  }

}