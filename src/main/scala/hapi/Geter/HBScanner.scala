/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供保存获取数据属性设置的类，继承了Scan
 *
 * 保存了连接、表名字
 */

package hapi.Geter

import hapi.HBConnection
import org.apache.hadoop.hbase.client.Scan


class HBScanner(val cnt: HBConnection, val tb: String) extends Scan {
  def getConnection = this.cnt
  def table = this.tb
  def getTableByte = this.tb.getBytes()
}
