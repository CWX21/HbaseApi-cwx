/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供提供文件读写支持
 * 主要用于解析配置文件
 */

package hapi

import java.io.File
import scala.io.{BufferedSource, Source}
import scala.collection.mutable.{Map => MuMap}

object FileSupport {
  def fileExist(fileName: String): Boolean = {
    if (new File(fileName).exists()) true else false
  }

  /**
    * 读取文件的内容， 以键值对的形式返回
    * 异常时返回None
    *
    * @param fileName 文件名字
    * @param d, 切割键值对的分割符号
    * @param annotate, 注释行的标识，如果以annotate开头，跳过此行, 默认是'#'
    * @return
    */
  def readFile2kv(fileName: String, d: String, annotate: Char = '#'): Option[Map[String, String]] = {
    var source: BufferedSource = null
    var ret: Option[Map[String, String]] = None
    try {
      source = Source.fromFile(fileName)
      val lines = source.getLines()
      val kvs = MuMap[String, String]()
      for (line <- lines) {
        val lineContent = line.trim
        if (lineContent.length > 0 && lineContent.charAt(0) != annotate) { //空行和注释行跳过
        val kv = lineContent.split(d, 2)
          kvs += (kv(0).trim -> kv(1).trim)
        }
      }
      ret = Some(kvs.toMap)
    } catch {
      case ex: Exception =>
        HBError.logError(ex.getStackTraceString)
        ret = None
    } finally {
      if (source != null) source.close()
    }
    ret
  }
}