package com.openbean.bd.unsupervisedlearning
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

//"/Users/ondrej.machacek/Projects/TMobile/data/unsupervised/flowgraph_20190205-0000_20190211-0000__cust_exp_weekly_aggregations"
class DataReader(implicit spark: SparkSession) extends Logger {
  def readData(filename: String) : DataFrame = {
    val data =spark
      .read
      .parquet(filename)
      .withColumn(ContractKPIsColumns.clv_agg.toString,
        when(col(ContractKPIsColumns.clv_agg.toString).equalTo(lit(-1)), lit(1))
          .otherwise(col(ContractKPIsColumns.clv_agg.toString)))
      .withColumn(UsageKPIsColumns.calls_data_ratio.toString, col(UsageKPIsColumns.calls_sum.toString)/col(UsageKPIsColumns.data_records_sum.toString))
      .withColumn(UsageKPIsColumns.data_sessions.toString, col(UsageKPIsColumns.data_volume_sum.toString)/col(UsageKPIsColumns.data_records_sum.toString))
      .withColumn(UsageKPIsColumns.lte_ratio.toString, col(UsageKPIsColumns.LTE_data_records_sum.toString)/col(UsageKPIsColumns.data_records_sum.toString))
      .withColumn(CXKPIsColumns.cex_tel_per_call_avg.toString, col(CXKPIsColumns.cex_tel_per_call_avg.toString)*100)
      .withColumn(CXKPIsColumns.cex_tel_per_call_max.toString, col(CXKPIsColumns.cex_tel_per_call_max.toString)*100)
      .withColumn(CXKPIsColumns.cex_tel_per_sec_avg.toString, col(CXKPIsColumns.cex_tel_per_sec_avg.toString)*60)
      .withColumn(CXKPIsColumns.cex_tel_per_sec_max.toString, col(CXKPIsColumns.cex_tel_per_sec_max.toString)*60)
      .withColumn(CXKPIsColumns.cex_browse_per_dv_avg.toString, col(CXKPIsColumns.cex_browse_per_dv_avg.toString)*1024)
      .withColumn(CXKPIsColumns.cex_browse_per_dv_max.toString, col(CXKPIsColumns.cex_browse_per_dv_max.toString)*1024)
      .withColumn(CXKPIsColumns.cex_data_per_dv_avg.toString, col(CXKPIsColumns.cex_data_per_dv_avg.toString)*1024)
      .withColumn(UsageKPIsColumns.data_volume_sum.toString, col(UsageKPIsColumns.data_volume_sum.toString)/1024/1024/1024)
      .withColumn(UsageKPIsColumns.data_sessions.toString, col(UsageKPIsColumns.data_sessions.toString)/1024)

    data.printSchema()

    data.select(CXKPIsColumns.avg_thp_dl_mbps.toString).distinct().show()

    data
  }
}
