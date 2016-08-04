/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供保存获取数据属性设置的类
 *
 * 保存了连接、表名字、最大版本
 */

package hapi.Geter

import hapi.HBConnection

/**
  * 下一个版本做成extends Get的类
  */
class HBGeter(val cnt: HBConnection, val tb: String) {
  private var _maxVersion = 1

  def getConnection = this.cnt
  def table = this.tb
  def getTableByte = this.tb.getBytes()

  /**
    * 设置获取的最大版本数
    * @return
    */
  def maxVersion = this._maxVersion
  def maxVersion_=(max: Int) = this._maxVersion = max
}
