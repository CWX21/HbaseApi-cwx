/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供一个hbase 列簇的抽象
 * 一个列簇里可以有多个列，且每个列都是独一无二的
 * 列名字可以为""
 */

package hapi.HBColumn

import scala.collection.mutable.ArrayBuffer

final class HBColumnFamily {
  private var family: String = ""
  private val columnCells = ArrayBuffer[HBColumnCell]()

  def this(family: String) = {
    this()
    this.family = family
  }

  def getFamily = this.family
  def getFamilyByte = this.family.getBytes()
  def getColumnCells = this.columnCells

  /**
    * 判断一个 列 是否存在
    * @param qualifier 列名字
    * @return
    */
  def columnCellExist(qualifier: String): Boolean = {
    columnCells foreach { cell =>
      if (cell.getQualifier == qualifier) return true
    }
    false
  }

  /**
    * 加入一个列， 如果这个已经存在，不会加入
    * @param qualifier 列名字
    * @param value 列值
    * @return
    */
  def addColumnCell(qualifier: String, value: String) = {
    if (!columnCellExist(qualifier)) {
      val cell = new HBColumnCell(qualifier, value)
      columnCells += cell
    }
  }

  def addColumnCell(value: String) = {
    if (!columnCellExist("")) {
      val cell = new HBColumnCell("", value)
      columnCells += cell
    }
  }

}