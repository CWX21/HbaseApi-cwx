/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供对hbase数据的读（扫描）功能
 * 主要实现的是scan hbase
 */

package hapi

import hapi.Geter.HBScanner
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConverters._


case class HBFamilyQualifier(family: Array[Byte], qualifier: Array[Byte])

/**
  * Created by lele on 15-12-8.
  */
class HObtainer {
  private def getHbTable(cnt: HBConnection, tableName: TableName): HTable = {
    cnt.getExecutors match {
      case None => cnt.getHBConnection.get.getTable(tableName).asInstanceOf[HTable]
      case Some(exec) => cnt.getHBConnection.get.getTable(tableName, exec).asInstanceOf[HTable]
    }
  }

  /**
    * 每次调用完后要close ResultScanner
    * @param scanResult 扫描结果
    * @return
    */
  def closeScanner(scanResult: ResultScanner): Boolean = {
    if (scanResult != null) {
      try {
        scanResult.close()
        true
      } catch {
        case ex: Exception =>
          HBError.logError(ex.getMessage)
          false
      }
    } else true
  }

  /**
    * 默认的输出函数
    * @param it 要输出的数据
    * @param hbcs 对应的列簇和列名字
    */
  private def printScanResult(it: Iterator[Result], hbcs: Array[HBFamilyQualifier]) = {
    while (it.hasNext) {
      val r = it.next()
      println(s"row key : ${Bytes.toString(r.getRow)}")
      hbcs foreach { hbc =>
        println(s"${Bytes.toString(hbc.family)}:${Bytes.toString(hbc.qualifier)}  " +
          s": ${Bytes.toString(r.getValue(hbc.family, hbc.qualifier))}")
      }
    }
  }

  /**
    * 解析经过scanner反回来的结果
    * @param scanResult 扫描结果
    * @param hbcs 对应的列簇和列名字
    * @param func 用来解析结果的函数，默认为printScanResult， 用户可以自己编写，一般会返回json
    * @return
    */
  def resolveScanResult(scanResult: ResultScanner, hbcs: Array[HBFamilyQualifier],
                        func: (Iterator[Result], Array[HBFamilyQualifier]) => Any = printScanResult): Option[Any] = {
    if (scanResult != null) {
      try {
        val ret = func(scanResult.iterator().asScala, hbcs)
        Some(ret)
      } catch {
        case ex: Exception =>
          HBError.logError(ex.getMessage)
          None
      }
    } else {
      None
    }
  }

  /**
    * 对表进行scan
    * @param hbScanner 扫描类
    * @return
    */
  def scan(hbScanner: HBScanner): Option[ResultScanner] = {
    var table: HTable = null
    try {
      val tableName = TableName.valueOf(hbScanner.table)
      table = getHbTable(hbScanner.getConnection, tableName)
      val rs = table.getScanner(hbScanner)
      Some(rs)
    } catch {
      case ex: Exception =>
        HBError.logError(ex.getMessage)
        None
    } finally {
      if (table != null) table.close()
    }
  }
}
