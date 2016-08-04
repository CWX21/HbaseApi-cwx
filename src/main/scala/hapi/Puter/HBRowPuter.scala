/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供出插入时一行的属性的记录类 继承于HBPuter
 */


package hapi.Puter

import hapi.HBColumn.HBColumnRow
import hapi.HBConnection

import scala.collection.mutable.ArrayBuffer

final class HBRowPuter(connection: HBConnection, val tableName: String) extends HBPuter(connection, tableName) {
  private var _data: TraversableOnce[HBColumnRow] = List[HBColumnRow]()

  def data = this._data
  def data_=(putdata: TraversableOnce[HBColumnRow]) = this._data = putdata
}