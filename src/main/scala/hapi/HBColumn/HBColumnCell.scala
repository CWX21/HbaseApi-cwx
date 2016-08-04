/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供一个hbase cell的抽象
 */

package hapi.HBColumn

/**
  * Created by lele on 15-11-21.
  */

final class HBColumnCell {
  private var qualifier: String = ""  //列名字
  private var value: String = ""      //列值

  def this(qualifier: String) = {
    this()
    this.qualifier = if (qualifier == Unit.toString()) "" else qualifier
  }

  def this(qualifier: String, value: String) = {
    this()
    this.qualifier = if (qualifier == Unit.toString()) "" else qualifier
    this.value = value
  }

  def getQualifierByte = {
     this.qualifier.getBytes()
  }

  def getQualifier = {
     this.qualifier
  }

  def getValue = this.value
  def getValueByte = this.value.getBytes()

}
