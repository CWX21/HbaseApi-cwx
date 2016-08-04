/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供一个hbase 一行的抽象
 * 当这个表只有一个列簇的时候，使用这个类进行出入比较高效
 */

package hapi.HBColumn

import scala.collection.mutable.ArrayBuffer

/**
  * 当表只有一个family的时候， 利用这个HBRowCell直接插入一个family会比较高效
  */
final class HBRowCell {
  private var rowKey = ""
  private val rowCells = ArrayBuffer[HBColumnCell]()

  def this(rowKey: String) = {
    this()
    this.rowKey = rowKey
  }

  def getRowCells = this.rowCells
  def getRowKey = this.rowKey
  def getRowKeyByte = this.rowKey.getBytes()

  def rowCellExist(qualifier: String): Boolean = {
    rowCells foreach { cell =>
      if (cell.getQualifier == qualifier) return true
    }
    false
  }

  def addRowCell(hBColumnCell: HBColumnCell) = {
    if (!rowCellExist(hBColumnCell.getQualifier)) {
      rowCells += hBColumnCell
    }
  }

  def addRowCell(qualifier: String, value: String) = {
    if (!rowCellExist(qualifier)) {
      val cell = new HBColumnCell(qualifier, value)
      rowCells += cell
    }
  }

}
