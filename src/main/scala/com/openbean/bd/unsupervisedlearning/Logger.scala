package com.openbean.bd.unsupervisedlearning

import org.slf4j.LoggerFactory

trait Logger {
  lazy val logger = LoggerFactory.getLogger(getClass)
}
