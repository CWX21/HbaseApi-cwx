/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供保存获取数据属性设置的类，继承了HBGeter
 *
 * 保存了连接、表名字、列簇、最大版本
 */

package hapi.Geter

import hapi.HBConnection

class HBQualifierGeter(cnt: HBConnection, tb: String) extends HBGeter(cnt, tb) {
  private var _family: String = ""

  def this(cnt: HBConnection, tb: String, family: String) = {
    this(cnt, tb)
    this._family = family
  }

  def family = this._family
  def getFamilyByte = this._family.getBytes()
}
