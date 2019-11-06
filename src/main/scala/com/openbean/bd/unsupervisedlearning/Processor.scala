package com.openbean.bd.unsupervisedlearning

import com.openbean.bd.unsupervisedlearning.supporting.{CXKPIsModel, ContractKPIsModel, CorrelatedColumns, DimensionAll, DimensionCPX, DimensionUsage, Logger, UsageKPIsModel}

abstract class Processor(dataReader: DataReader) extends Logger {
  val columnsForClustering = Map(DimensionAll -> (CXKPIsModel.getModelCols ++ UsageKPIsModel.getModelCols),
    DimensionCPX -> CXKPIsModel.getModelCols,
    DimensionUsage -> UsageKPIsModel.getModelCols)

  val allFields = columnsForClustering(DimensionAll) ++ ContractKPIsModel.getModelCols ++ CorrelatedColumns.getRemovedCols

  lazy val dataRaw = dataReader
    .readData()
    .select("user_id", allFields: _*)

  def run() : Unit
}
