package com.openbean.bd.unsupervisedlearning

import com.openbean.bd.unsupervisedlearning.supporting._
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{DataFrame, SaveMode}

trait Writer extends Logger {
  def writeCrossDimensionStats(data: DataFrame, dimensions: Array[Dimension]) : Unit
  def writeClusterData(dataClustered: Map[Dimension, (DataFrame, KMeansModel)], dataRaw: DataFrame): Unit
  def writeSummaryRaw(data: DataFrame): Unit
  def writeResult(data: DataFrame): Unit
}

class ResultWriter(crossDimensionalStatsOutput: String, rawSummaryOutput: String, clusterStatsOutput: String, resultFile: String, mode: String) extends Writer {


  private def writeExcelOrParquet(data: DataFrame, filename: String, saveMode: SaveMode, sheetName: Option[String] = None ) = {

    mode match {
      case "excel" =>
        data
          .coalesce(1)
          .write
          .format("com.crealytics.spark.excel")
          .option("sheetName", sheetName.getOrElse("Default"))
          .option("useHeader", "true")
          .option("dateFormat", "yy-mmm-d")
          .option("timestampFormat", "mm-dd-yyyy hh:mm:ss")
          .mode(saveMode)
          .save(filename)
      case "parquet" =>
        data
          .coalesce(1)
          .write
          .mode(saveMode)
          .parquet(filename)
    }
  }


  override def writeCrossDimensionStats(data: DataFrame, dimensions: Array[Dimension]
                              ) = {
    val grouped = data
      .groupBy(dimensions.head.clusteringColumnName, dimensions.tail.map(_.clusteringColumnName): _*)
      .count()
    grouped.show(false)


    writeExcelOrParquet(grouped, crossDimensionalStatsOutput, SaveMode.Overwrite, Some("Cross-cluster-stats"))
  }

  override def writeClusterData(dataClustered: Map[Dimension, (DataFrame, KMeansModel)], dataRaw: DataFrame): Unit = {

    def aggregated(dim: Dimension, array: Array[String]): DataFrame = {
      logger.info(s"Aggregating on cluster ${dim.name} and calculating means")
      val grouped = dataRaw
        .select(dim.clusteringColumnName, array: _*)
        .groupBy(dim.clusteringColumnName)
        .mean(array: _*)

      logger.info("Getting counts")
      val counts = Clustering.getClusterStats(dataRaw, dim)

      logger.info("Joining meand with counts")
      grouped
        .join(counts, dim.clusteringColumnName)
        .sort(dim.clusteringColumnName)
    }

    logger.info("Starting preparation for writing cluster data")
    for {i <- dataClustered.keys} {
      logger.info(s"Data calculation for dimension ${i.name}")
      val result = i match {
        case DimensionCPX => aggregated(DimensionCPX, CXKPIsModel.getModelCols ++ ContractKPIsModel.getModelCols ++ CXCorrelatedColumns.getRemovedCols)
        case DimensionUsage => aggregated(DimensionUsage, UsageKPIsModel.getModelCols ++ ContractKPIsModel.getModelCols ++ UsageCorrelatedColumns.getRemovedCols)
        case DimensionContract => aggregated(DimensionContract, ContractKPIsModel.getModelCols)
        case DimensionAll => aggregated(DimensionAll, CXKPIsModel.getModelCols ++ UsageKPIsModel.getModelCols ++ ContractKPIsModel.getModelCols ++ CorrelatedColumns.getRemovedCols)
      }
      logger.info(s"Cluster data for ${i.name} ready to be written into a file")

      writeExcelOrParquet(result,clusterStatsOutput, SaveMode.Append,Some(s"Cluster ${i.name}"))

      logger.info("Writing cluster data DONE")

    }
  }

  override def writeSummaryRaw(data: DataFrame) = {
    writeExcelOrParquet(data.summary(), rawSummaryOutput,SaveMode.Overwrite,Some("Summary All Fields" ))
  }

  override def writeResult(data: DataFrame) = {
    data
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(resultFile)
  }

}