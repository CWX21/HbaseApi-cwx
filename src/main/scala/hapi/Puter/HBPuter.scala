/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供出插入时属性的记录类
 * 记录了缓冲区大小、wal的类型、ttl、表名字
 */

package hapi.Puter

import hapi.HBConnection
import org.apache.hadoop.hbase.client.Durability

/**
  * Created by lele on 15-11-26.
  */
class HBPuter(connection: HBConnection, tableName: String) {
  protected var _bufSize: Long = 1024 * 128  //默认为128k的缓冲
  /**
    * ASYNC_WAL ： 当数据变动时，异步写WAL日志
    * SYNC_WAL ： 当数据变动时，同步写WAL日志
    * FSYNC_WAL ： 当数据变动时，同步写WAL日志，并且，强制将数据写入磁盘
    * SKIP_WAL ： 不写WAL日志
    * USE_DEFAULT ： 使用HBase全局默认的WAL写入级别，即 SYNC_WAL
    */
  protected var _durability: Durability = Durability.USE_DEFAULT  //默认为hbase的默认级别，先写日志（WAL）
  protected var _ttl: Long = -1

  def bufSize = this._bufSize
  def bufSize_=(size: Long) = if (size > 0) _bufSize = size

  def durability = this._durability
  def durability_=(d: Durability) = _durability = d

  def ttl = this._ttl
  def ttl_=(t: Long) = if (t > 0) this._ttl = t

  def getTableName = this.tableName
  def getConnection = this.connection

}
