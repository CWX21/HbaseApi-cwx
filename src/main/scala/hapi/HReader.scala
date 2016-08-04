/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供对hbase数据的读功能
 * 主要实现的是geter
 * 其实在内部 get 也是一个scan
 */

package hapi

import hapi.Geter.{HBGeter, HBQualifierGeter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, TableName}
import org.apache.hadoop.hbase.client.{HTable, Result, Get}
import scala.collection.JavaConverters._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ArrayBuffer

class HReader {
  def resolveResult(result: Result) = {
    println(Bytes.toString(result.getRow))
    for (cell <- result.rawCells()) {
      println(Bytes.toString(CellUtil.cloneFamily(cell)))
      println(Bytes.toString(CellUtil.cloneQualifier(cell)))
      println(Bytes.toString(CellUtil.cloneValue(cell)))
    }
  }

  def resolveResults(results: Array[Result]) = {
    results foreach { result =>
      println(Bytes.toString(result.getRow))
      for (cell <- result.rawCells()) {
        println(Bytes.toString(CellUtil.cloneFamily(cell)))
        println(Bytes.toString(CellUtil.cloneQualifier(cell)))
        println(Bytes.toString(CellUtil.cloneValue(cell)))
      }
      println("\n----------------------------------\n")
    }
  }

  /**
    * 将结果解析成json，当多个version的时候，只能显示一个version
    * @param result 读取的结果
    * @return
    */
  def resolveResult2Json(result: Result): String = {
    var ret = parse(s"""{ "rowkey":"${Bytes.toString(result.getRow)}" }""")
    for (cell <- result.rawCells()) {
      val cf = Bytes.toString(CellUtil.cloneFamily(cell))
      val qul = Bytes.toString(CellUtil.cloneQualifier(cell))
      val value = Bytes.toString(CellUtil.cloneValue(cell))
      val jsonNode = parse(s"""{ "$cf":{ "$qul":"$value" } }""")
      ret = ret merge jsonNode
    }
    compact(render(ret))
  }

  /**
    * 将结果数组解析为json，当数组非常大的时候，效率会很低效
    * 终究是要组成json, 会出现栈溢出的异常
    * @param results 读取的结果
    * @return
    */
  def resolveResults2Json(results: Array[Result]): String = {
    var ret = parse("""{  }""")
    for (result <- results) {
      var row = parse(s"""{  }""")
      for (cell <- result.rawCells()) {
        val cf = Bytes.toString(CellUtil.cloneFamily(cell))
        val qul = Bytes.toString(CellUtil.cloneQualifier(cell))
        val value = Bytes.toString(CellUtil.cloneValue(cell))
        val jsonNode = parse(s"""{ "$cf":{ "$qul":"$value" } }""")
        row = row merge jsonNode
      }
      ret = ret merge render(Bytes.toString(result.getRow) -> row)
    }
    compact(render(ret))
  }

  private def getHbTable(cnt: HBConnection, tableName: TableName): HTable = {
    cnt.getExecutors match {
      case None => cnt.getHBConnection.get.getTable(tableName).asInstanceOf[HTable]
      case Some(exec) => cnt.getHBConnection.get.getTable(tableName, exec).asInstanceOf[HTable]
    }
  }

  private def setGeter[hbGeter <: HBGeter](hbGeter: HBGeter, geter: Get) = {
    geter.setMaxVersions(hbGeter.maxVersion)
  }

  private def setGeter[hbGeter <: HBGeter, geters <: Traversable[Get]](hbGeter: HBGeter,
                                                                          geters: Traversable[Get]) = {
    geters.par foreach { geter =>
      geter.setMaxVersions(hbGeter.maxVersion)
    }
  }

  /**
    * 获取一行对应的所有数据
    * @param rowKey 行键
    * @return
    */
  def getRow(hbGeter: HBGeter, rowKey: String): Option[Result] = {
    var table: HTable = null
    var ret: Option[Result] = None
    try {
      val tableName = TableName.valueOf(hbGeter.tb)
      table = getHbTable(hbGeter.cnt, tableName)
      val geter = new Get(rowKey.getBytes())
      setGeter(hbGeter, geter)
      ret = Option(table.get(geter))
    } catch {
      case ex: Exception =>
        HBError.logError(ex.getMessage)
        ret = None
    } finally {
      if (table != null) table.close()
    }
    ret
  }

  /**
    * 获取多个rowkey对应的所有数据
    * @param rowKeys 行键
    * @return
    */
  def getRows(hbGeter: HBGeter, rowKeys: Traversable[String]): Option[Array[Result]] = {
    var table: HTable = null
    var ret: Option[Array[Result]] = None
    try {
      val tableName = TableName.valueOf(hbGeter.tb)
      val geters = rowKeys.map(rowKey => new Get(rowKey.getBytes())).toList
      table = getHbTable(hbGeter.cnt, tableName)
      setGeter(hbGeter, geters)
      ret = Option(table.get(geters.asJava))  //一次获取多行
    } catch {
      case ex: Exception =>
        HBError.logError(ex.getMessage)
        ret = None
    } finally {
      if (table != null) table.close()
    }
    ret
  }

  /**
    * 获取一个rowkey对应的一个指定列的数据
    * @param rowKey 行键
    * @param qualifier 列名字
    * @return
    */
  def getRowByQualifier(hbGeter: HBQualifierGeter, rowKey: String, qualifier: String): Option[Result] = {
    var table: HTable = null
    var ret: Option[Result] = None
    try {
      val tableName = TableName.valueOf(hbGeter.tb)
      table = getHbTable(hbGeter.cnt, tableName)
      val geter = new Get(rowKey.getBytes())
      setGeter(hbGeter, geter)
      geter.addColumn(hbGeter.getFamilyByte, qualifier.getBytes())
      val result = table.get(geter)
      ret = Option(result)
    } catch {
      case ex: Exception =>
        HBError.logError(ex.getMessage)
        ret = None
    } finally {
      if (table != null) table.close()
    }
    ret
  }

  /**
    * 获取一个列的多行数据
    * @param rowKeys 行键
    * @param qualifier 列名字
    * @return
    */
  def getRowsByQualifier(hbGeter: HBQualifierGeter, rowKeys: Traversable[String],
                         qualifier: String): Option[Array[Result]] = {
    var table: HTable = null
    var ret: Option[Array[Result]] = None
    try {
      val geters = ArrayBuffer[Get]()
      val tableName = TableName.valueOf(hbGeter.tb)
      table = getHbTable(hbGeter.cnt, tableName)
      val cf = hbGeter.getFamilyByte
      val col = qualifier.getBytes()

      for (rowkey <- rowKeys) {
        val get = new Get(rowkey.getBytes())
        get.addColumn(cf, col)
        geters += get
      }

      setGeter(hbGeter, geters)
      ret = Option(table.get(geters.asJava))
    } catch {
      case ex: Exception =>
        HBError.logError(ex.getMessage)
        ret = None
    } finally {
      if (table != null) table.close()
    }
    ret
  }

  /**
    * 获取一个rowkey下多个列的数据
    * @param rowKey 行键
    * @param qualifiers 列名字
    * @return
    */
  def getRowByQualifiers(hbGeter: HBQualifierGeter, rowKey: String, qualifiers: Traversable[String]): Option[Result] = {
    var table: HTable = null
    var ret: Option[Result] = None
    try {
      val cf = hbGeter.getFamilyByte
      val tableName = TableName.valueOf(hbGeter.tb)
      table = getHbTable(hbGeter.cnt, tableName)
      val geter = new Get(rowKey.getBytes())
      setGeter(hbGeter, geter)
      for (qualifier <- qualifiers) {
        geter.addColumn(cf, qualifier.getBytes())
      }
      ret = Option(table.get(geter))
    } catch {
      case ex: Exception =>
        HBError.logError(ex.getMessage)
        ret = None
    } finally {
      if (table != null) table.close()
    }
    ret
  }

  /**
    * 获取多行的多个列簇数据
    * @param rowCells 行键
    * @return
    */
  def getRowsByQualifiers(hbGeter: HBQualifierGeter,
                          rowCells: Map[String, Traversable[String]]): Option[Array[Result]] = {
    var table: HTable = null
    var ret: Option[Array[Result]] = None
    try {
      val geters = ArrayBuffer[Get]()
      val cf = hbGeter.getFamilyByte
      val tableName = TableName.valueOf(hbGeter.tb)
      table = getHbTable(hbGeter.cnt, tableName)

      rowCells.keys foreach { rowkey =>
        val get = new Get(rowkey.getBytes())
        rowCells(rowkey) foreach { qualifier =>
          get.addColumn(cf, qualifier.getBytes())
        }
        geters += get
      }
      setGeter(hbGeter, geters)
      ret = Option(table.get(geters.asJava))
    } catch {
      case ex: Exception =>
        HBError.logError(ex.getMessage)
        ret = None
    } finally {
      if (table != null) table.close()
    }
    ret
  }

  /**
    * 获取一行的一个family
    * @param hbGeter 用于保存连接、表名字的get
    * @param family 列簇
    * @param row 行健
    * @return
    */
  def getRowByFamily(hbGeter: HBGeter, family: String, row: String): Option[Result] = {
    var table: HTable = null
    var ret: Option[Result] = None

    try {
      val tableName = TableName.valueOf(hbGeter.table)
      table = getHbTable(hbGeter.cnt, tableName)
      val get = new Get(row.getBytes())
      get.addFamily(family.getBytes())
      ret = Option(table.get(get))
    } catch {
      case ex: Exception =>
        HBError.logError(ex.getMessage)
        ret = None
    } finally {
      if (table != null) table.close()
    }
    ret
  }

  /**
    * 获取多个行下的同一个family
    * @param hbGeter 用于保存连接、表名字的get
    * @param family 列簇
    * @param rows 行键
    * @return
    */
  def getRowsByFamily(hbGeter: HBGeter, family: String, rows: Traversable[String]): Option[Array[Result]] = {
    var table: HTable = null
    var ret: Option[Array[Result]] = None

    try {
      val tableName = TableName.valueOf(hbGeter.table)
      val geters = ArrayBuffer[Get]()
      val cf = family.getBytes()
      table = getHbTable(hbGeter.cnt, tableName)
      rows foreach { rowkey =>
        val get = new Get(rowkey.getBytes())
        get.addFamily(cf)
        geters += get
      }
      ret = Option(table.get(geters.asJava))
    } catch {
      case ex: Exception =>
        HBError.logError(ex.getMessage)
        ret = None
    } finally {
      if (table != null) table.close()
    }
    ret
  }

  /**
    * 获取一行的多个family
    * @param hbGeter 用于保存连接、表名字的get
    * @param familys 列簇
    * @param row 行键
    * @return
    */
  def getRowByFamilys(hbGeter: HBGeter, familys: Traversable[String], row: String): Option[Result] = {
    var table: HTable = null
    var ret: Option[Result] = None

    try {
      val tableName = TableName.valueOf(hbGeter.table)
      table = getHbTable(hbGeter.cnt, tableName)

      val get = new Get(row.getBytes())
      familys foreach { family =>
        get.addFamily(family.getBytes())
      }
      ret = Option(table.get(get))
    } catch {
      case ex:Exception =>
        HBError.logError(ex.getMessage)
        ret = None
    }
    ret
  }

  /**
    * 获取多个行的对个family
    * @param hbGeter 用于保存连接、表名字的get
    * @param familys 列簇
    * @param rows 行键
    * @return
    */
  def getRowsByFamilys(hbGeter: HBGeter, familys: Traversable[String],
                       rows: Traversable[String]): Option[Array[Result]] = {
    var table: HTable = null
    var ret: Option[Array[Result]] = None

    try {
      val geters = ArrayBuffer[Get]()
      val tableName = TableName.valueOf(hbGeter.table)
      table = getHbTable(hbGeter.cnt, tableName)
      rows foreach { rowkey =>
        val get = new Get(rowkey.getBytes())
        familys foreach { cf =>
          get.addFamily(cf.getBytes())
        }
        geters += get
      }
      ret = Option(table.get(geters.asJava))
    } catch {
      case ex:Exception =>
        HBError.logError(ex.getMessage)
        ret = None
    } finally {
      if (table != null) table.close()
    }
    ret
  }

}