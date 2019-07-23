package com.openbean.bd.unsupervisedlearning
import java.io._

import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


class Process(data: DataFrame) extends Logger {


  val (cpxDataVectorised, contractKPIVectorised, usageKPIDataVectorised) = getDataVectorised(data)

  val allFields = CXKPIsModel.getModelCols ++ UsageKPIsModel.getModelCols ++ ContractKPIsModel.getModelCols
  val allVectorised = Clustering.vectorise(data, allFields)

  def getDataVectorised(data: DataFrame) : (DataFrame,DataFrame,DataFrame) = {
    (Clustering.vectorise(Clustering.oneHot(data,ContractKPIsColumns.clv_agg.toString),CXKPIsModel.getModelCols),
      Clustering.vectorise(data,ContractKPIsModel.getModelCols),
      Clustering.vectorise(data,UsageKPIsModel.getModelCols))
  }

  private def doClustering3D(cpxData: DataFrame, contractData: DataFrame, usageData: DataFrame, clusters: Int) : Map[Dimension,(DataFrame, KMeansModel)] = {
    val cpxClusterModel = Clustering.doKMeans(clusters,cpxData)
    val contractKPIModel = Clustering.doKMeans(clusters,contractData)
    val usageKPIModel = Clustering.doKMeans(clusters,usageData)


    val usageKPIClusteredData = usageKPIModel.transform(usageData)
    val cpxClusteredData = cpxClusterModel.transform(cpxData)
    val contractKPIClusteredData = contractKPIModel.transform(contractData)

    Map(DimensionCPX -> (cpxClusteredData,cpxClusterModel ),
      DimensionContract -> (contractKPIClusteredData, contractKPIModel),
      DimensionUsage -> (usageKPIClusteredData, usageKPIModel))
  }

  //outputFilenameStats - "/Users/ondrej.machacek/tmp/clustersStatsPCA.csv"
  //outputFilenameData  - "/Users/ondrej.machacek/tmp/clusterDetailsPCA.csv"
  def doWithPCA(cpxDataVectorised: DataFrame, contractKPIVectorised : DataFrame, usageKPIDataVectorised : DataFrame, pcaVars: Map[Dimension, Int], clusters: Int, outputFilenameStats: String, outputFilenameData: String)(implicit spark: SparkSession) = {

    def columns(dim: Dimension) : Array[String] = {
      (1 to pcaVars(dim)).map(_.toString()).toArray
    }

    val cpxData = PCAHelper.getPCA(pcaVars(DimensionCPX.name),cpxDataVectorised)
    val usageData = PCAHelper.getPCA(pcaVars(DimensionUsage.name),usageKPIDataVectorised)
    val contractData = PCAHelper.getPCA(pcaVars(DimensionContract.name),contractKPIVectorised)

    val dataClustered = doClustering3D(cpxData,contractData,usageData, 5  )

    ResultWriter.writeClusterStats(dataClustered, columns(DimensionCPX), columns(DimensionUsage), columns(DimensionContract), outputFilenameStats)

    ResultWriter.writeClustersData(cpxClusteredData,
      usageKPIClusteredData,
      contractKPIClusteredData, outputFilenameData
    )
  }

  def doWithoutPCA() = {




    val cpxClusterModel = Clustering.doKMeans(5,cpxDataVectorised)
    val contractKPIModel = Clustering.doKMeans(5,contractKPIVectorised)
    val usageKPIModel = Clustering.doKMeans(5,usageKPIDataVectorised)


    val usageKPIClusteredData = usageKPIModel.transform(usageKPIDataVectorised)
    val cpxClusteredData = cpxClusterModel.transform(cpxDataVectorised)
    val contractKPIClusteredData = contractKPIModel.transform(contractKPIVectorised)

    ResultWriter.writeClusterStats(cpxClusteredData,
      usageKPIClusteredData,
      contractKPIClusteredData,
      cpxClusterModel,
      usageKPIModel,
      contractKPIModel,
      CXKPIsModel.getModelCols,
      UsageKPIsModel.getModelCols,
      ContractKPIsModel.getModelCols,
      "/Users/ondrej.machacek/tmp/clustersStatsNoPCA.csv")
    ResultWriter.writeClustersData(cpxClusteredData,
      usageKPIClusteredData,
      contractKPIClusteredData,
      "/Users/ondrej.machacek/tmp/clusterDetailsNoPCA.csv"
    )
  }

  def doAllWithPCA() = {
    //option("header","true").mode(SaveMode.Overwrite).

    val scaled = Clustering.scale(allVectorised)

    val cpaData = PCAHelper.getPCA(5,scaled)

    val model = Clustering.doKMeans(9,cpaData)

    val transformed = model.transform(cpaData)

    val clusterStats = Clustering.getClusterStats(transformed,model.clusterCenters)

    val pw = new PrintWriter(new File("/Users/ondrej.machacek/tmp/AllFieldsClusterStatsPCA.csv"))

    pw.write(s"clusterID;cluster center (1,2,3,4,5); count;\n")

    for (i <- 0 to (clusterStats.length-1) ) {
      pw.write(s"${i};(${clusterStats(i).clusterCenter.toArray.mkString(",")});${clusterStats(i).count};\n")
    }
    pw.close

    transformed


  }

  def doAllWithoutPCA() = {
    //option("header","true").mode(SaveMode.Overwrite).

    val scaled = Clustering.scale(allVectorised)

    val model = Clustering.doKMeans(9,scaled)

    val transformed = model.transform(scaled)

    val clusterStats = Clustering.getClusterStats(transformed,model.clusterCenters)

    val pw = new PrintWriter(new File("/Users/ondrej.machacek/tmp/AllFieldsClusterStatsNoPCA.csv"))

    pw.write(s"clusterID;cluster center (${allFields.mkString(",")}); count;\n")

    for (i <- 0 to (clusterStats.length-1) ) {
      pw.write(s"${i};(${clusterStats(i).clusterCenter.toArray.mkString(",")});${clusterStats(i).count};\n")
    }
    pw.close

    transformed
  }

}