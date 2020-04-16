package org.grapheco.commons.util

import org.slf4j.LoggerFactory

/**
  * Created by bluejoe on 2019/5/24.
  */
trait Logging {
  val logger = LoggerFactory.getLogger(this.getClass)
}
