package com.openbean.bd.unsupervisedlearning
import com.openbean.bd.unsupervisedlearning.supporting.{CXKPIsColumns, CXKPIsModel, ContractKPIsColumns, Logger, UsageKPIsColumns}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Means(meanthp: Double, meandvsum: Double, meancallsum: Double, meandrsum: Double, meanltesum: Double)

//"/Users/ondrej.machacek/Projects/TMobile/data/unsupervised/flowgraph_20190205-0000_20190211-0000__cust_exp_weekly_aggregations"
class DataReader(filename: String)(implicit spark: SparkSession) extends Logger {
  def readData() : DataFrame = {

    import spark.implicits._

    val rawData = spark
      .read
      .parquet(filename)
      .withColumn(ContractKPIsColumns.clv_agg.toString,
        when(col(ContractKPIsColumns.clv_agg.toString).equalTo(lit(-1)), lit(1))
          .otherwise(col(ContractKPIsColumns.clv_agg.toString)))
       .na.fill(0,CXKPIsModel.getModelCols.filter(_.contains("cex")))

    //rawData.columns.foreach(println(_))

    val means = rawData
      .select(
        mean(col(CXKPIsColumns.avg_thp_dl_mbps.toString)).alias("meanthp"),
        mean(col(UsageKPIsColumns.data_volume_sum.toString)).alias("meandvsum"),
        mean(col(UsageKPIsColumns.calls_sum.toString)).alias("meancallsum"),
        mean(col(UsageKPIsColumns.data_records_sum.toString)).alias("meandrsum"),
        mean(col(UsageKPIsColumns.LTE_data_records_sum.toString)).alias("meanltesum")
      )
      .as[Means].take(1)

    //println(means(0))


    val nafixed = rawData
      .na.fill(means(0).meanthp, Seq(CXKPIsColumns.avg_thp_dl_mbps.toString))
      .na.fill(means(0).meancallsum, Seq(UsageKPIsColumns.calls_sum.toString))
      .na.fill(means(0).meandrsum,Seq(UsageKPIsColumns.data_records_sum.toString))
      .na.fill(means(0).meandvsum, Seq(UsageKPIsColumns.data_volume_sum.toString))
      .na.fill(means(0).meanltesum,Seq(UsageKPIsColumns.LTE_data_records_sum.toString))

    //nafixed.na.

    val ret = nafixed
      .withColumn(UsageKPIsColumns.calls_data_ratio.toString, col(UsageKPIsColumns.calls_sum.toString).divide(col(UsageKPIsColumns.data_records_sum.toString)))
      .withColumn(UsageKPIsColumns.data_sessions.toString, col(UsageKPIsColumns.data_volume_sum.toString).divide(col(UsageKPIsColumns.data_records_sum.toString)))
      .withColumn(UsageKPIsColumns.lte_ratio.toString, col(UsageKPIsColumns.LTE_data_records_sum.toString).divide(col(UsageKPIsColumns.data_records_sum.toString)))
      .withColumn(CXKPIsColumns.cex_tel_per_call_avg.toString, col(CXKPIsColumns.cex_tel_per_call_avg.toString)*100)
      .withColumn(CXKPIsColumns.cex_tel_per_call_max.toString, col(CXKPIsColumns.cex_tel_per_call_max.toString)*100)
      .withColumn(CXKPIsColumns.cex_tel_per_sec_avg.toString, col(CXKPIsColumns.cex_tel_per_sec_avg.toString)*60)
      .withColumn(CXKPIsColumns.cex_tel_per_sec_max.toString, col(CXKPIsColumns.cex_tel_per_sec_max.toString)*60)
      .withColumn(CXKPIsColumns.cex_browse_per_dv_avg.toString, col(CXKPIsColumns.cex_browse_per_dv_avg.toString)*1024)
      .withColumn(CXKPIsColumns.cex_browse_per_dv_max.toString, col(CXKPIsColumns.cex_browse_per_dv_max.toString)*1024)
      .withColumn(CXKPIsColumns.cex_data_per_dv_avg.toString, col(CXKPIsColumns.cex_data_per_dv_avg.toString)*1024)
      .withColumn(UsageKPIsColumns.data_volume_sum.toString, col(UsageKPIsColumns.data_volume_sum.toString)/1024/1024/1024)
      .withColumn(UsageKPIsColumns.data_sessions.toString, col(UsageKPIsColumns.data_sessions.toString)/1024)
      .na.fill(0, Seq(UsageKPIsColumns.calls_data_ratio.toString, UsageKPIsColumns.data_sessions.toString, UsageKPIsColumns.lte_ratio.toString))
      .withColumn(ContractKPIsColumns.kuendigung3_12_3.toString, when(col(ContractKPIsColumns.kuendigung3_12.toString).equalTo(lit(3)), 1).otherwise(lit(0)))
      .withColumn(ContractKPIsColumns.kuendigung3_12_13.toString, when(col(ContractKPIsColumns.kuendigung3_12.toString).equalTo(lit(13)), 1).otherwise(lit(0)))

    //ret.select(UsageKPIsColumns.calls_data_ratio.toString).filter(s"${UsageKPIsColumns.calls_data_ratio.toString} is null").show(false)
    //ret.select(UsageKPIsColumns.data_sessions.toString).filter(s"${UsageKPIsColumns.data_sessions.toString} is null").show(false)
    //ret.select(ContractKPIsColumns.kuendigung3_12_13.toString).distinct().show(false)
    //ret.select(ContractKPIsColumns.kuendigung3_12_3.toString).distinct().show(false)
    //ret.select(ContractKPIsColumns.kuendigung3_12.toString).distinct().show(false)

    ret


    //data.printSchema()

    //data.select(CXKPIsColumns.avg_thp_dl_mbps.toString).distinct().show()

    //data
  }
}
