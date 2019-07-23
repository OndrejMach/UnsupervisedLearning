package com.openbean.bd.unsupervisedlearning

trait Dimension {
  val name : String
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