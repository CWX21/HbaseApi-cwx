/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序主要实现管理hbase表的功能
 * 提供表的family的增删改查功能
 */

package hapi

import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.{KeepDeletedCells, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.Admin

/**
  * 扩展于列簇描述符，代表一个列簇
  * 利用map， 简单方便地设置列簇的属性
  * @param fName 列簇名字
  */
class HFamily(fName: String) extends HColumnDescriptor(fName.getBytes) {

  def this(fName: String, attrs: Map[String, String]) = {
    this(fName)
    setHColumnDescriptorAttrs(attrs)
  }

  /**
    * 值为true， false的解析器
    * @param attr 属性
    * @return
    */
  private def resolveBlooeanAttrValue(attr: String): Any = {
    attr match {
      case "true" => true
      case "false" => false
      case _ => false
    }
  }

  /**
    * 设置BloomFilter，多个选项，所以分开处理
    * @param option BloomFilter 选项
    * @return
    */
  private def setBloomFilter(option: String) = {
    option match {
      case "row"  => this.setBloomFilterType(BloomType.ROW)
      case "rowcol" => this.setBloomFilterType(BloomType.ROWCOL)
      case _ => this.setBloomFilterType(BloomType.NONE)
    }
  }

  /**
    * 设置KeepDeletedCells属性，因为有多个选项，所以分开处理
    * @param option KeepDeletedCells 选项
    * @return
    */
  private def setKDC(option: String) = {
    option match {
      case "true" => this.setKeepDeletedCells(KeepDeletedCells.TRUE)
      case "false" => this.setKeepDeletedCells(KeepDeletedCells.FALSE)
      case "ttl" => this.setKeepDeletedCells(KeepDeletedCells.TTL)
      case _ => this.setKeepDeletedCells(KeepDeletedCells.FALSE)  //默认是false的
    }
  }

  private def doSetHColumnDescriptorAttrs(attrs: Map[String, String], attrName: String) = {
    attrName match {
      case "maxversions" => this.setMaxVersions(attrs(attrName).toInt)
      case "minversions" => this.setMinVersions(attrs(attrName).toInt)
      case "ttl" => this.setTimeToLive(attrs(attrName).toInt)
      case "blocksize" => this.setBlocksize(attrs(attrName).toInt)
      case "inmem" => this.setInMemory(resolveBlooeanAttrValue(attrs(attrName)).asInstanceOf[Boolean])
      case "bloomfilter" => setBloomFilter(attrs(attrName))
      case "scope" => this.setScope(attrs(attrName).toInt)
      case "keepdeletecells" => setKDC(attrs(attrName))
      case "blockcache" => this.setBlockCacheEnabled(resolveBlooeanAttrValue(attrs(attrName)).asInstanceOf[Boolean])
      case _ =>
    }
  }

  /**
    * 方便文件读入时，组成[string, strig]类型的情况
    * @param attrs 属性
    */
  def setHColumnDescriptorAttrs(attrs: Map[String, String]): Boolean = {
    try {
      attrs.keys foreach { attrName => doSetHColumnDescriptorAttrs(attrs, attrName) }
      true
    } catch {
      case ex: Exception =>
        HBError.logError(ex.getMessage)
        false
    }
  }

  /**
    * 支持原生设置
    * @param attrs 要设置的属性
    * @param attrName 属性名字
    * @tparam A 类型参数
    * @return
    */
  private def doSetHColumnDescriptor[A <: Any](attrs: Map[String, A], attrName: String) = {
    attrName match {
      case "maxversions" => this.setMaxVersions(attrs(attrName).asInstanceOf[Int])
      case "minversions" => this.setMinVersions(attrs(attrName).asInstanceOf[Int])
      case "ttl" => this.setTimeToLive(attrs(attrName).asInstanceOf[Int])
      case "blocksize" => this.setBlocksize(attrs(attrName).asInstanceOf[Int])
      case "inmem" => this.setInMemory(attrs(attrName).asInstanceOf[Boolean])
      case "bloomfilter" => this.setBloomFilterType(attrs(attrName).asInstanceOf[BloomType])
      case "scope" => this.setScope(attrs(attrName).asInstanceOf[Int])
      case "keepdeletecells" => this.setKeepDeletedCells(attrs(attrName).asInstanceOf[KeepDeletedCells])
      case "blockcache" => this.setBlockCacheEnabled(attrs(attrName).asInstanceOf[Boolean])
      case _ =>
    }
  }

  /**
    * 将原生api支持的设置选项跟值组成map
    * @param attrs 属性
    */
  def setHColumnDescriptor(attrs: Map[String, Any]) = {
    try {
      attrs.keys foreach { attrName => doSetHColumnDescriptor(attrs, attrName) }
      true
    } catch {
      case ex: Exception =>
        HBError.logError(ex.getMessage)
        false
    }
  }

  def getHFamilyName = this.fName

}

object HManager {
  import scala.language.implicitConversions
  implicit def toString(tableNames: Array[TableName]): Array[String] = {
    tableNames.map(name => name.getNameAsString)
  }

  def listTableNames(admin: Admin): Array[TableName] = {
    admin.listTableNames()
  }

  def listTableNamesAsString(admin: Admin): Array[String] = {
    admin.listTableNames()
  }

  def tableExist(admin: Admin, tb: String): Boolean = {
    val tableName = TableName.valueOf(tb)
    admin.tableExists(tableName)
  }

  /**
    * 创建表，是object HManager 创建表的根函数
    * 如果要创建的表已经存在，返回true
    * @param admin 管理者
    * @param tb 表名字
    * @param hFamilys 多个列簇
    * @return 失败或者发生异常的时候返回false
    */
  def createTable(admin: Admin, tb: String, hFamilys: TraversableOnce[HFamily]): Boolean = {
    try {
      val table = TableName.valueOf(tb)
      if (admin.tableExists(table)) true
      else {
        val tableDesc = new HTableDescriptor(table)
        hFamilys foreach{ cf =>
          tableDesc.addFamily(cf)
        }
        admin.createTable(tableDesc)
        true
      }
    } catch {
      case ex: Exception =>
        HBError.logError(ex.getMessage)
        false
    }
  }

  def createTable(admin: Admin, tb: String, hFamily: HFamily): Boolean = {
    val hFamilys = Array[HFamily](hFamily)
    createTable(admin, tb, hFamilys)
  }

  /**
    * 强制性地创建表，如果表存在，把表删除，这个功能主要用于测试代码
    * 应用于有多个列簇的情况
    * @param admin 管理者
    * @param tb 表名字
    * @param hFamilys 对个列簇
    * @return 失败或者发生异常的时候返回false
    */
  def createTableForce(admin: Admin, tb: String, hFamilys: TraversableOnce[HFamily]): Boolean = {
    dropTable(admin, tb)
    createTable(admin, tb, hFamilys)
  }

  /**
    * 强制性地创建表，如果表存在，把表删除，这个功能主要用于测试代码
    * 应用于只有一个列簇的情况
    * @param admin 管理者
    * @param tb 表名字
    * @param hFamily 多个列簇
    * @return 失败或者发生异常的时候返回false
    */
  def createTableForce(admin: Admin, tb: String, hFamily: HFamily): Boolean = {
    dropTable(admin, tb)
    createTable(admin, tb, hFamily)
  }

  /**
    * 销毁数据表
    * 发生异常的时候，返回false
    * 表不存在的时候返回true
    */
  def dropTable(admin: Admin, tb: String): Boolean = {
    try {
      val table = TableName.valueOf(tb)
      if (admin.tableExists(table)) {
        admin.disableTable(table)
        admin.deleteTable(table)
      }
      true
    } catch {
      case ex:Exception =>
        HBError.logError(ex.getMessage)
        false
    }
  }

  /**
    * 加入一个列簇
    * 如果表不存在，返回false，并不是抛出异常
    * @param admin 管理者
    * @param tb 表名字
    * @param hFamily 列簇
    * @return
    */
  def addFamily(admin: Admin, tb: String, hFamily: HFamily): Boolean = {
    val tableName = TableName.valueOf(tb)
    if (admin.tableExists(tableName)) {
      try {
        admin.disableTable(tableName)
        val tableDescr = admin.getTableDescriptor(tableName)
        if (tableDescr.hasFamily(hFamily.getHFamilyName.getBytes)) {
          admin.enableTable(tableName)  //必须把表解锁
          true
        }
        else {
          tableDescr.addFamily(hFamily)
          admin.modifyTable(tableName, tableDescr)
          admin.enableTable(tableName)
          true
        }
      } catch {
        case ex: Exception =>
          admin.enableTable(tableName)
          HBError.logError(ex.getMessage)
          false
      }
    }else {
      HBError.logError(s"can't find table: $tb")
      false
    }
  }

  /**
    * 加入多个列簇
    * 如果表不存在，返回false，并不是抛出异常
    * @param admin 管理者
    * @param tb 表名字
    * @param hFamilys 多个列簇
    * @return
    */
  def addFamilys(admin: Admin, tb: String, hFamilys: TraversableOnce[HFamily]): Boolean = {
    val tableName = TableName.valueOf(tb)
    if (admin.tableExists(tableName)) {
      try {
        admin.disableTable(tableName)
        val tableDescr = admin.getTableDescriptor(tableName)
        hFamilys.foreach({ hFamily =>
          if (!tableDescr.hasFamily(hFamily.getHFamilyName.getBytes)) { //存在的列簇不在再建
            tableDescr.addFamily(hFamily)
          }
        })
        admin.modifyTable(tableName, tableDescr)
        admin.enableTable(tableName)
        true
      } catch {
        case ex: Exception =>
          admin.enableTable(tableName)
          HBError.logError(ex.getMessage)
          false
      }
    }else {
      HBError.logError(s"can't find table: $tb")
      false
    }
  }

  /**
    * 删除一个列簇
    * 如果表不存在，返回false，并不是抛出异常
    * 如果列簇本身不存在，返回true
    * @return
    */
  def deleteFamily(admin: Admin, tb: String, hFamily: HFamily): Boolean = {
    val tableName = TableName.valueOf(tb)
    if (admin.tableExists(tableName)) {
      try {
        admin.disableTable(tableName)
        val tableDescr = admin.getTableDescriptor(tableName)
        if (!tableDescr.hasFamily(hFamily.getHFamilyName.getBytes)) {
          admin.enableTable(tableName)
          true
        }else {
          tableDescr.removeFamily(hFamily.getHFamilyName.getBytes())
          admin.modifyTable(tableName, tableDescr)
          admin.enableTable(tableName)
          true
        }
      } catch {
        case ex: Exception =>
          admin.enableTable(tableName)
          HBError.logError(ex.getMessage)
          false
      }
    }else {
      HBError.logError(s"can't find table: $tb")
      false
    }
  }

  /**
    * 删除多个个列簇
    * 如果表不存在，返回false，并不是抛出异常
    * 如果列簇本身不存在，忽略处理
    * @return
    */
  def deleteFamilys(admin: Admin, tb: String, hFamilys: TraversableOnce[HFamily]): Boolean = {
    val tableName = TableName.valueOf(tb)
    if (admin.tableExists(tableName)) {
      try {
        admin.disableTable(tableName)
        val tableDescr = admin.getTableDescriptor(tableName)
        hFamilys foreach { hFamily =>
          if (tableDescr.hasFamily(hFamily.getHFamilyName.getBytes())) {
            tableDescr.removeFamily(hFamily.getHFamilyName.getBytes())
          }
        }

        admin.modifyTable(tableName, tableDescr)
        admin.enableTable(tableName)
        true
      } catch {
        case ex: Exception =>
          admin.enableTable(tableName)
          HBError.logError(ex.getMessage)
          false
      }
    }else {
      HBError.logError(s"can't find table: $tb")
      false
    }
  }

  /**
    * 判断一个列簇是否存在
    * 如果表不存在，返回false，异常返回false，错误信息保存在HBError里
    * @param admin 管理者
    * @param tb 表名字
    * @param familyName 列簇
    * @return
    */
  def familyExist(admin: Admin, tb: String, familyName: String): Boolean = {
    val tableName = TableName.valueOf(tb)
    if (admin.tableExists(tableName)) {
      try {
        val tableDescr = admin.getTableDescriptor(tableName)
        if (tableDescr.hasFamily(familyName.getBytes())) true
        else {
          false
        }
      } catch {
        case ex: Exception =>
          HBError.logError(ex.getMessage)
          false
      }
    }else {
      HBError.logError(s"can't find table: $tb")
      false
    }
  }

  /**
    * 更新一个列簇的属性
    * 如果表不存在，返回false，异常返回false，错误信息保存在HBError里
    * @param admin 管理者
    * @param tb 表名字
    * @param hFamily 列簇
    * @return
    */
  def updateFamilyArrts(admin: Admin, tb: String, hFamily: HFamily): Boolean = {
    val tableName = TableName.valueOf(tb.getBytes())
    if (admin.tableExists(tableName)) {
      try {
        admin.disableTable(tableName)
        val tableDescr = admin.getTableDescriptor(tableName)
        if (!tableDescr.hasFamily(hFamily.getHFamilyName.getBytes())) {
          admin.enableTable(tableName)
          HBError.logError(s"can't find family: ${hFamily.getHFamilyName}")
          false
        }else {
          tableDescr.modifyFamily(hFamily)
          admin.modifyTable(tableName, tableDescr)
          admin.enableTable(tableName)
          true
        }
      } catch {
        case ex: Exception =>
          admin.enableTable(tableName)
          HBError.logError(ex.getMessage)
          false
      }
    }else {
      HBError.logError(s"can't find table: $tb")
      false
    }
  }

  /**
    * 更新多个列簇的属性
    * 如果表不存在，返回false，异常返回false，错误信息保存在HBError里
    * @param admin 管理者
    * @param tb 表名字
    * @param hFamilys 列簇名字
    * @return
    */
  def updateFamilyArrts(admin: Admin, tb: String, hFamilys: TraversableOnce[HFamily]): Boolean = {
    val tableName = TableName.valueOf(tb.getBytes())
    if (admin.tableExists(tableName)) {
      try {
        admin.disableTable(tableName)
        val tableDescr = admin.getTableDescriptor(tableName)
        hFamilys.foreach({ hFamily =>
          if (!tableDescr.hasFamily(hFamily.getHFamilyName.getBytes())) {
            admin.enableTable(tableName)
            HBError.logError(s"can't find family: ${hFamily.getHFamilyName}")
            return false
          }
        })

        hFamilys.foreach({ hFamily =>
          tableDescr.modifyFamily(hFamily)
        })
        admin.modifyTable(tableName, tableDescr)
        admin.enableTable(tableName)
        true
      } catch {
        case ex: Exception =>
          admin.enableTable(tableName)
          HBError.logError(ex.getMessage)
          false
      }
    }else {
      HBError.logError(s"can't find table: $tb")
      false
    }
  }

}