package com.openbean.bd.unsupervisedlearning.supporting

object CXKPIsColumns extends Enumeration {
  type CXKPIsColumns
  val cex_tel_per_call_avg : CXKPIsColumns.Value = Value("cex_tel_per_call_avg")
  val cex_tel_per_call_max: CXKPIsColumns.Value = Value("cex_tel_per_call_max")
  val cex_tel_per_sec_avg: CXKPIsColumns.Value = Value("cex_tel_per_sec_avg")
  val cex_tel_per_sec_max: CXKPIsColumns.Value = Value("cex_tel_per_sec_max")
  val cex_browse_per_dv_avg: CXKPIsColumns.Value = Value("cex_browse_per_dv_avg")
  val cex_browse_per_dv_max: CXKPIsColumns.Value = Value("cex_browse_per_dv_max")
  val cex_browse_per_session_avg: CXKPIsColumns.Value = Value("cex_browse_per_session_avg")
  val cex_browse_per_session_max: CXKPIsColumns.Value = Value("cex_browse_per_session_max")
  val cex_data_per_dv_avg: CXKPIsColumns.Value = Value("cex_data_per_dv_avg")
  val avg_thp_dl_mbps: CXKPIsColumns.Value = Value("avg_thp_dl_mbps")
}

object CXKPIsModel {
  val getModelCols = Array(CXKPIsColumns.cex_tel_per_call_avg.toString,
    CXKPIsColumns.cex_tel_per_call_max.toString,
    CXKPIsColumns.cex_tel_per_sec_avg.toString,
    CXKPIsColumns.cex_tel_per_sec_max.toString,
    CXKPIsColumns.cex_browse_per_dv_avg.toString,
    CXKPIsColumns.cex_browse_per_dv_max.toString,
    CXKPIsColumns.cex_browse_per_session_avg.toString,
    CXKPIsColumns.cex_browse_per_session_max.toString,
    CXKPIsColumns.cex_data_per_dv_avg.toString,
    CXKPIsColumns.avg_thp_dl_mbps.toString)
}

object UsageKPIsColumns extends Enumeration {
  type UsageKPIsColumns
  val calls_sum : UsageKPIsColumns.Value = Value("calls_sum")
  val data_records_sum: UsageKPIsColumns.Value = Value("data_records_sum")
  val LTE_data_records_sum: UsageKPIsColumns.Value = Value("LTE_data_records_sum")
  val data_volume_sum: UsageKPIsColumns.Value = Value("data_volume_sum")
  val calls_data_ratio: UsageKPIsColumns.Value = Value("calls_data_ratio")
  val data_sessions: UsageKPIsColumns.Value = Value("data_sessions")
  val lte_ratio:  UsageKPIsColumns.Value = Value("lte_ratio")
}

object UsageKPIsModel {
  val getModelCols = Array(UsageKPIsColumns.calls_sum.toString,
    UsageKPIsColumns.data_records_sum.toString,
    UsageKPIsColumns.LTE_data_records_sum.toString,
    UsageKPIsColumns.data_volume_sum.toString,
    UsageKPIsColumns.calls_data_ratio.toString,
    UsageKPIsColumns.data_sessions.toString,
    UsageKPIsColumns.lte_ratio.toString
  )
}

object ContractKPIsColumns extends Enumeration {
  type ContractKPIsColumns
  val kek_4 :ContractKPIsColumns.Value = Value("kek_4")
  val clv_agg :ContractKPIsColumns.Value = Value("clv_agg")
  val magenta1 :ContractKPIsColumns.Value = Value("magenta1")
  val streaming_streamon :ContractKPIsColumns.Value = Value("streaming_streamon")
  val kuendigung3_12 :ContractKPIsColumns.Value = Value("kuendigung3_12")
  val kuendigung3_12_3:ContractKPIsColumns.Value = Value("kuendigung3_12_3")
  val kuendigung3_12_13:ContractKPIsColumns.Value = Value("kuendigung3_12_13")
  val morpu_avg :ContractKPIsColumns.Value = Value("morpu_avg")
  val clv123 :ContractKPIsColumns.Value = Value("clv123")
  val high_val_cust :ContractKPIsColumns.Value = Value("high_val_cust")
}

object ContractKPIsModel {
  val getModelCols = Array(//ContractKPIsColumns.kek_4.toString,
    //ContractKPIsColumns.clv_agg.toString,
    ContractKPIsColumns.magenta1.toString,
    ContractKPIsColumns.streaming_streamon.toString,
    //ContractKPIsColumns.kuendigung3_12.toString,
    ContractKPIsColumns.kuendigung3_12_3.toString,
    ContractKPIsColumns.kuendigung3_12_13.toString,
    ContractKPIsColumns.morpu_avg.toString,
    ContractKPIsColumns.clv123.toString
    //ContractKPIsColumns.high_val_cust.toString
  )
}
