/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供对hbase数据的写功能
 */

package hapi

import hapi.Puter.{HBRowPuter, HBFamilyPuter}

import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import hapi.HBColumn.HBColumnRow

import scala.collection.mutable.ArrayBuffer

class HWriter {
  /**
    * 这个函数主要用来测试的， 大量数据的时候还是使用缓存
    * @param admin 管理者
    * @param cnt 连接
    * @param tb 表名字
    * @param columnrow 保存列簇跟行键
    * @return
    */
  def putOne(admin: Admin, cnt: Connection, tb: String, columnrow: HBColumnRow): Boolean = {
    var status = false
    var table: Table = null
    val tableName = TableName.valueOf(tb)

    if (admin.tableExists(tableName)) {
      try {
        table = cnt.getTable(tableName)
        val familys = columnrow.getFamilys
        val row = new Put(columnrow.getRowKeyByte)
        familys foreach { cf =>
          val columnCells = cf.getColumnCells
          val cfFamilyByte = cf.getFamilyByte
          columnCells foreach { columnCell =>
            row.addColumn(cfFamilyByte, columnCell.getQualifierByte, columnCell.getValueByte)
          }
        }

        table.put(row)
        status = true
      } catch {
        case ex: Exception =>
          HBError.logError(ex.getMessage)
          status = false
      } finally { /* 无论如何都要释放资源 */
        if (table != null) table.close()
      }
      status
    }else {
      HBError.logError(s"can't find table: $tb")
      false
    }
  }

  private def getHbTable(cnt: HBConnection, tableName: TableName): HTable = {
    cnt.getExecutors match {
      case None => cnt.getHBConnection.get.getTable(tableName).asInstanceOf[HTable]
      case Some(exec) => cnt.getHBConnection.get.getTable(tableName, exec).asInstanceOf[HTable]
    }
  }

  private def doPutRows(table: HTable, puter: HBRowPuter): Boolean = {
    val puts = ArrayBuffer[Put]()     //缓存所有put
    table.setAutoFlush(false, false)  //关键点1
    table.setWriteBufferSize(puter.bufSize) //关键点2

    puter.data foreach { columnRow =>
      val familys = columnRow.getFamilys
      val row = new Put(columnRow.getRowKeyByte)
      familys foreach { cf =>
        val columnCells = cf.getColumnCells
        val cfFamilyByte = cf.getFamilyByte

        columnCells foreach { columnCell =>
          row.addColumn(cfFamilyByte, columnCell.getQualifierByte, columnCell.getValueByte)
        }  //end columnCells.foreach
      }  //end familys.foreach
      /**
        * toDo
        * 实现多个设置选项如ttl
        */
      row.setDurability(puter.durability)  //设置wal的类型
      puts += row
    }  //end columnRows.par.foreach

    table.put(puts.asJava)  //批量put
    table.flushCommits()
    true
  }

  /**
    * hbase表多family的情况下使用这个api
    * 不建议一个hbase的表有多个family
    * @param puter  保存了连接跟表名字
    * @return
    */
  def putRows(puter: HBRowPuter): Boolean = {
    var status = false
    var table: HTable = null
    val tableName = TableName.valueOf(puter.tableName)

    try {
      if (puter.getConnection.getAdmin.get.tableExists(tableName)) {
        table = getHbTable(puter.getConnection, tableName)
        doPutRows(table, puter)
        status = true
      }else {
        HBError.logError(s"can't find table: ${puter.getTableName}")
        status = false
      }
    } catch {
      case ex:Exception =>
        HBError.logError(ex.getMessage)
        status = false
    } finally {
      if (table != null) table.close()
    }
    status
  }

  /**
    * 插入操作的实现函数
    * @param table 保存表的数据结构
    * @param puter 保存了表名字，列簇等
    * @return
    */
  private def doPutRowsAtFamily(table: HTable, puter: HBFamilyPuter): Boolean = {
    val puts = ArrayBuffer[Put]()     //缓存所有put
    val familyByte = puter.getFamilByte
    table.setAutoFlush(false, false)  //关键点1
    table.setWriteBufferSize(puter.bufSize) //关键点2

    puter.data foreach { rowCell =>
      val row = new Put(rowCell.getRowKeyByte)
      rowCell.getRowCells foreach {  cell =>
        row.addColumn(familyByte, cell.getQualifierByte, cell.getValueByte)
      }

      /**
        * toDo
        * 实现多个设置选项如ttl
        */
      row.setDurability(puter.durability)
      puts += row
    }

    table.put(puts.asJava)
    table.flushCommits()
    true
  }

  /**
    * 当数据表只有一个family的时候，使用这个api
    * 其实大部分的情况下，hbase的表都应该只有一个family，hbase对多family支持不是非常好
    * @param puter 保存了表名字，列簇等
    * @return
    */
  def putRowsAtFamily(puter: HBFamilyPuter): Boolean = {
    var status = false
    var table: HTable = null
    val tableName = TableName.valueOf(puter.getTableName)

    try {
      if (puter.getConnection.getAdmin.get.tableExists(tableName)) {
        //是否开启多线程，使用线程池插入，在10w数据下好似没有什么大的区别
        table = getHbTable(puter.getConnection, tableName)
        doPutRowsAtFamily(table, puter)
        status = true
      }else {
        HBError.logError(s"can't find table: ${puter.getTableName}")
        status = false
      }
    } catch {
      case ex: Exception =>
        HBError.logError(ex.getMessage)
        status = false
    } finally {
      if (table != null) table.close()
    }
    status
  }

}