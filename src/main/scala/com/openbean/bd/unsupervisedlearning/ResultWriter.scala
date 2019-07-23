package com.openbean.bd.unsupervisedlearning

import java.io._

import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

//"/Users/ondrej.machacek/tmp/clustersStats.csv"
//"/Users/ondrej.machacek/tmp/clusterDetails.csv"

object ResultWriter extends Logger {

  def writeCSV(pw: PrintWriter,clusterStats: Seq[ClusterStats]) = {
    for (i <- 0 to (clusterStats.length-1) ) {
      pw.write(s"${i};(${clusterStats(i).clusterCenter.toArray.mkString(",")});${clusterStats(i).count}\n")
    }
  }

  def writeClusterStats(clusterData: Map[Dimension,(DataFrame, KMeansModel)],
                        columnsCpx: Array[String],
                        columnsUsage: Array[String],
                        columnsContract: Array[String],
                        filename: String)(implicit spark: SparkSession) = {


    val cpxClusterStats = Clustering.getClusterStats(clusterData(DimensionCPX)._1,clusterData(DimensionCPX)._2.clusterCenters)
    val usageClusterStats = Clustering.getClusterStats(clusterData(DimensionUsage)._1,clusterData(DimensionUsage)._2.clusterCenters)
    val contractClusterStats = Clustering.getClusterStats(clusterData(DimensionContract)._1,clusterData(DimensionContract)._2.clusterCenters)

    val pw = new PrintWriter(new File(filename))
    pw.write(s"clusterID;cluster center CXKPIs (${columnsCpx.mkString(",")}); count\n")
    writeCSV(pw,cpxClusterStats )
    pw.write(s"cluster center UsageKPIs (${columnsUsage.mkString(",")});count\n")
    writeCSV(pw,usageClusterStats )
    pw.write(s"cluster center ContractKPIs (${columnsContract.mkString(",")});count \n")
    writeCSV(pw, contractClusterStats)

    pw.close
  }

  def writeClustersData(cpxData: DataFrame,
                        usageKPIData: DataFrame,
                        contractData: DataFrame,
                        filename: String) = {

    val toJoinU = usageKPIData
      .drop("features")
      .withColumnRenamed("prediction", "cluster_id_usage")
    val toJoinCPX = cpxData
      .drop("features")
      .withColumnRenamed("prediction", "cluster_id_cpx")
    val toJoinContr = contractData
      .drop("features")
      .withColumnRenamed("prediction", "cluster_id_contract")



    val joined = toJoinU
      .join(toJoinCPX,"user_id")
      .join(toJoinContr, "user_id")
      .filter("user_id is not null")


    joined.printSchema()

    case class ClusterDetail(prediction_usage: Int,prediction_cpx: Int,prediction_contract: Int, count: Long)

    val grouped = joined.select("cluster_id_usage","cluster_id_cpx","cluster_id_contract" )
      .groupBy("cluster_id_usage","cluster_id_cpx","cluster_id_contract")
      .count()
    //.as[ClusterDetail]
    grouped.show(false)
    //println(grouped.count)

    grouped.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite).csv(filename)
  }

}