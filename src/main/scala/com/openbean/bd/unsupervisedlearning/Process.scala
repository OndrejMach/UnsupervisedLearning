package com.openbean.bd.unsupervisedlearning
import java.io._

import com.openbean.bd.unsupervisedlearning.supporting.{CXKPIsModel, ContractKPIsColumns, ContractKPIsModel, Dimension, DimensionCPX, DimensionContract, DimensionUsage, Logger, UsageKPIsModel}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


class Process(dataReader: DataReader) extends Logger {


  def getDataVectorised3D(data: DataFrame) : (DataFrame,DataFrame,DataFrame) = {
    (Clustering.vectorise(Clustering.oneHot(data,ContractKPIsColumns.clv_agg.toString),CXKPIsModel.getModelCols),
      Clustering.vectorise(data,ContractKPIsModel.getModelCols),
      Clustering.vectorise(data,UsageKPIsModel.getModelCols))
  }

  private def doClustering(data: DataFrame, clusters: Int) = {
    val model = Clustering.doKMeans(clusters, data)

    val transformed = model.transform(data)

    (model, transformed)
  }


  private def doClustering3D(cpxData: DataFrame, contractData: DataFrame, usageData: DataFrame, clusters: Int) : Map[Dimension,(DataFrame, KMeansModel)] = {

    val (cpxClusterModel, cpxClusteredData) = doClustering(cpxData, clusters)
    val (contractKPIModel,usageKPIClusteredData) = doClustering(contractData,clusters)
    val (usageKPIModel,contractKPIClusteredData) = doClustering(usageData,clusters)

    Map(DimensionCPX -> (cpxClusteredData,cpxClusterModel ),
      DimensionContract -> (contractKPIClusteredData, contractKPIModel),
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

  def do3D(cpxDataVectorised: DataFrame, contractKPIVectorised : DataFrame, usageKPIDataVectorised : DataFrame,
                   clusters: Int, //outputFilenameStats: String, outputFilenameData: String,
                   cpxCols:Array[String] = CXKPIsModel.getModelCols, usageCols: Array[String] = UsageKPIsModel.getModelCols, contractCols:Array[String] = ContractKPIsModel.getModelCols)(implicit spark: SparkSession) = {



    //val dataClustered = doClustering3D(cpxDataVectorised,contractKPIVectorised,usageKPIDataVectorised,clusters   )
    doClustering3D(cpxDataVectorised,contractKPIVectorised,usageKPIDataVectorised,clusters   )

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

  def doAll(data: DataFrame, clusters: Int)(implicit spark: SparkSession) = {
    //option("header","true").mode(SaveMode.Overwrite).

    doClustering(data, clusters)

    //ResultWriter.writeClusterStats(transformed,model,allFields,filename)
  }

  def run()= {

    val data= dataReader.readData("/Users/ondrej.machacek/Projects/TMobile/data/unsupervised/flowgraph_20190205-0000_20190211-0000__cust_exp_weekly_aggregations")
    lazy val (cpxDataVectorised, contractKPIVectorised, usageKPIDataVectorised) = getDataVectorised3D(data)

    val allFields = CXKPIsModel.getModelCols ++ UsageKPIsModel.getModelCols ++ ContractKPIsModel.getModelCols
    lazy val allVectorised = Clustering.vectorise(data, allFields)


  }

}