package com.openbean.bd.unsupervisedlearning
import java.io._

import com.openbean.bd.unsupervisedlearning.supporting.{CXKPIsModel, ContractKPIsColumns, ContractKPIsModel, Dimension, DimensionCPX, DimensionContract, DimensionUsage, Logger, UsageKPIsModel}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml._


case class Stat(mean: Double, stddev: Double)

class Process(dataReader: DataReader)(implicit spark: SparkSession) extends Logger {


  def getDataVectorised3D(data: DataFrame) : (DataFrame,DataFrame) = {
    (Clustering.vectorise(Clustering.oneHot(data,ContractKPIsColumns.clv_agg.toString),CXKPIsModel.getModelCols),
     // Clustering.vectorise(data,ContractKPIsModel.getModelCols),
      Clustering.vectorise(data,UsageKPIsModel.getModelCols))
  }

  private def doClustering(data: DataFrame, clusters: Int) = {
    val model = Clustering.doKMeans(clusters, data)

    val transformed = model.transform(data)

    (model, transformed)
  }


  private def doClustering3D(cpxData: DataFrame, usageData: DataFrame, clusters: Int) : Map[Dimension,(DataFrame, KMeansModel)] = {

    val (cpxClusterModel, cpxClusteredData) = doClustering(cpxData, clusters)
    //val (contractKPIModel,usageKPIClusteredData) = doClustering(contractData,clusters)
    val (usageKPIModel,usageKPIClusteredData) = doClustering(usageData,clusters)

    Map(DimensionCPX -> (cpxClusteredData,cpxClusterModel ),
     // DimensionContract -> (contractKPIClusteredData, contractKPIModel),
      DimensionUsage -> (usageKPIClusteredData, usageKPIModel))
  }

  //outputFilenameStats - "/Users/ondrej.machacek/tmp/clustersStatsPCA.csv"
  //outputFilenameData  - "/Users/ondrej.machacek/tmp/clusterDetailsPCA.csv"
  //def doWithPCA(cpxDataVectorised: DataFrame, contractKPIVectorised : DataFrame, usageKPIDataVectorised : DataFrame, pcaVars: Map[Dimension, Int], clusters: Int, outputFilenameStats: String, outputFilenameData: String)(implicit spark: SparkSession) = {

  /*def doWithPCA(cpxDataVectorised: DataFrame, contractKPIVectorised : DataFrame, usageKPIDataVectorised : DataFrame, pcaVars: Map[Dimension, Int], clusters: Int)(implicit spark: SparkSession) = {
    def columns(dim: Dimension) : Array[String] = {
      (1 to pcaVars(dim)).map(_.toString()).toArray
    }

    val cpxData = PCAHelper.getPCA(pcaVars(DimensionCPX),cpxDataVectorised)
    val usageData = PCAHelper.getPCA(pcaVars(DimensionUsage),usageKPIDataVectorised)
    val contractData = PCAHelper.getPCA(pcaVars(DimensionContract),contractKPIVectorised)

    doWithoutPCA(cpxData,contractData,usageData,clusters,columns(DimensionCPX), columns(DimensionUsage), columns(DimensionContract) )
  }*/

  def do3D(cpxDataVectorised: DataFrame, usageKPIDataVectorised : DataFrame,
                   clusters: Int, //outputFilenameStats: String, outputFilenameData: String,
                   cpxCols:Array[String] = CXKPIsModel.getModelCols,
           usageCols: Array[String] = UsageKPIsModel.getModelCols)
          (implicit spark: SparkSession) = {



    //val dataClustered = doClustering3D(cpxDataVectorised,contractKPIVectorised,usageKPIDataVectorised,clusters   )
    doClustering3D(cpxDataVectorised,usageKPIDataVectorised,clusters   )

    //ResultWriter.writeClusterStats3D(dataClustered, cpxCols, usageCols, contractCols, outputFilenameStats)

    //ResultWriter.writeClustersData(dataClustered(DimensionCPX)._1,
    //  dataClustered(DimensionUsage)._1,
    //  dataClustered(DimensionContract)._1, outputFilenameData
   // )
  }

  /*def doAllWithPCA(data: DataFrame, clusters: Int, PCAvars: Int)(implicit spark: SparkSession) = {
    //option("header","true").mode(SaveMode.Overwrite).

    //val scaled = Clustering.scale(allVectorised)

    val cpaData = PCAHelper.getPCA(PCAvars,data)

    //val (model, transformed) =
     doClustering(cpaData, clusters)

    //val transformed = model.transform(cpaData)

    //ResultWriter.writeClusterStats(transformed,model,allFields,filename)
  }*/

  def doAll(data: DataFrame, clusters: Int) = {
    //option("header","true").mode(SaveMode.Overwrite).

    doClustering(data, clusters)

    //ResultWriter.writeClusterStats(transformed,model,allFields,filename)
  }

  def filterOutliers(fields: Array[String], data: DataFrame): DataFrame = {

    import spark.implicits._

    var tmp = data
    logger.info(s"With Outliers ${tmp.count()}")


    for (i <- fields) {
      logger.info(s"Outlier removal for ${i} count: ${tmp.count()}")
      val stats = tmp.select(mean(i).alias("mean"), stddev_pop(i).alias("stddev")).as[Stat].collect()(0)
      val min = stats.mean-50*stats.stddev
      val max = stats.mean+50+stats.stddev
      logger.info(s"${stats}")
      tmp = tmp.filter(col(i).geq(lit(min)) && col(i).leq(lit(max)))
      logger.info(s"Without outliers for ${i} count: ${tmp.count()}")
    }
    logger.info(s"After Outliers removal count ${tmp.count()}")
    tmp
  }


  def run()= {
    val allFields = CXKPIsModel.getModelCols ++ UsageKPIsModel.getModelCols ++ ContractKPIsModel.getModelCols

    //println(s"${allFields(0)}:::${allFields.tail.mkString(",")}")

    allFields.foreach(println(_))

    val dataRaw= dataReader
      .readData("/Users/ondrej.machacek/Projects/TMobile/data/unsupervised/part-00000-892b3c0f-ad5f-4ab9-aaa9-3c668a3be26d-c000.snappy.parquet")
      .select(allFields(0), allFields.tail :_*)

    dataRaw.printSchema()

    val data = filterOutliers(allFields, dataRaw)


    lazy val (cpxDataVectorised, usageKPIDataVectorised) = getDataVectorised3D(data)

    lazy val allVectorised = Clustering.vectorise(data, allFields)


    //println(data.select("cex_tel_per_call_avg").filter("cex_tel_per_call_avg is null").count())

    data.summary().repartition(1).write.option("header", "true").csv("/Users/ondrej.machacek/tmp/AllDesc.csv")

    val scaled = Clustering.scale(allVectorised).select("features")

    //scaled.printSchema()

    val vecToArray = udf( (xs: linalg.Vector) => xs.toArray )
    val dfArr = scaled.withColumn("featuresArr" , vecToArray(col("features")) )

    val sqlExpr = allFields.zipWithIndex.map{ case (alias, idx) => col("featuresArr").getItem(idx).as(alias) }

    val scaledDF = dfArr.select(sqlExpr : _*)

    scaledDF.summary().repartition(1).write.option("header", "true").csv("/Users/ondrej.machacek/tmp/AllDescScaled.csv")

    //scaledDF.show(truncate = false)



  }

}