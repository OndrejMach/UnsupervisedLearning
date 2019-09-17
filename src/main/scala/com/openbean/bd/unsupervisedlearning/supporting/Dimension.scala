package com.openbean.bd.unsupervisedlearning.supporting

trait Dimension {
  val name : String
  def clusteringColumnName: String = s"${COLUMN_NAME_PREFIX}${name}"
  val COLUMN_NAME_PREFIX = "cluster_"
}

object DimensionCPX extends Dimension {
  override val name: String = "cpx"
}

object DimensionUsage extends Dimension {
  override val name: String = "usage"
}

object DimensionContract extends Dimension {
  override val name: String = "contract"
}

object DimensionAll extends Dimension {
  override val name: String = "All"
}