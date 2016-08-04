/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供一个hbase 一行数据的抽象
 * 每一行可以又多个列簇（这个建表时不建议），每个列簇又可以有多个列
 */

package hapi.HBColumn

import scala.collection.mutable.ArrayBuffer

final class HBColumnRow {
  private var rowKey: String = ""
  private val familys = ArrayBuffer[HBColumnFamily]()

  def this(rowKey: String) = {
    this()
    this.rowKey = rowKey
  }

  def familyExist(familyName: String): Boolean = {
    familys foreach { family =>
      if (family.getFamily == familyName) return true
    }
    false
  }

  def addFamily(family: HBColumnFamily) = {
    if (!familyExist(family.getFamily)) {
      familys += family
    }
  }

  def addFamilys(addfamilys: TraversableOnce[HBColumnFamily]) = {
    for (cf <- addfamilys if !familyExist(cf.getFamily)) {
      familys += cf
    }
  }

  def getRowKey = this.rowKey
  def getRowKeyByte = this.rowKey.getBytes()
  def getFamilys: TraversableOnce[HBColumnFamily] = this.familys
}