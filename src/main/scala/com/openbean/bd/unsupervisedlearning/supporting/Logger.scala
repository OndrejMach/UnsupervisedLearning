package com.openbean.bd.unsupervisedlearning.supporting

import org.slf4j.LoggerFactory

trait Logger {
  lazy val logger = LoggerFactory.getLogger(getClass)
}
