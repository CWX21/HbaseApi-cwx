/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供出插入时Family属性的记录类 继承于HBPuter
 */

package hapi.Puter

import hapi.HBColumn.HBRowCell
import hapi.HBConnection

import scala.collection.TraversableOnce

/**
  * Created by lele on 15-11-26.
  */
final class HBFamilyPuter(connection: HBConnection, tableName: String,
                          family: String) extends HBPuter(connection, tableName) {
  private var _data: TraversableOnce[HBRowCell] = List[HBRowCell]()

  def data = this._data
  def data_=(putdata: TraversableOnce[HBRowCell]) = this._data = putdata

  def getFamily = this.family
  def getFamilByte = this.family.getBytes()
}