package com.keevol.kvectors.utils

import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.{Logger, LoggerFactory}

object Closables {
  private val logger: Logger = LoggerFactory.getLogger(Closables.getClass.getName)

  def closeWithLog(closeable: AutoCloseable): Unit = {
    try {
      if (closeable != null) {
        closeable.close()
      }
    } catch {
      case t: Throwable => logger.warn(s"exception on closing resource: ${ExceptionUtils.getStackTrace(t)}")
    }
  }

  def catchAndLog[T](op: => T): Unit = {
    try {
      op
    } catch {
      case t: Throwable => logger.warn(s"catchAndLog with exception: \n${ExceptionUtils.getStackTrace(t)}")
    }
  }
}