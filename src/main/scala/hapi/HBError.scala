/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供记录hapi运行过程中的错误信息
 */

package hapi

import scala.collection.mutable.ArrayBuffer

object HBError {
  val errors = ArrayBuffer[String]()

  def logError(err: String) = {
    if (err.length > 128) errors.clear()
    errors += err
  }

  def logErrors(errs: ArrayBuffer[String]) = {
    if (errors.length > 128) errors.clear()
    errors ++ errs
  }

  def getErrors: String = {
    var errorStr = ""
    errors.foreach({ e =>
      errorStr += s"$e\n"
    })
    errorStr
  }

  def cleanErrors() = {
    if (errors.nonEmpty) errors.clear()
  }

  def getAndCleanErrors(): String = {
    val errs = getErrors
    cleanErrors()
    errs
  }

  def getErrorSize: Int = errors.size
}
