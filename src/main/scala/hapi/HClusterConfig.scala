/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 配置集群的信息
 * zookeeper 信息、hbase master 信息等
 * 这些信息可以以kv的形式保存在一个文件中，用“=”分割，每个条配置占一行
 */

package hapi

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

class HClusterConfig {
  def getHbaseConfig: Option[Configuration] = Some(HBaseConfiguration.create())

  def getHbaseConfig(cfgInfo: Map[String, String]): Option[Configuration] = {
    val cfg = HBaseConfiguration.create()
    cfgInfo.keys foreach { cmd => cfg.set(cmd, cfgInfo(cmd)) }
    Some(cfg)
  }

  def getHbaseConfig(cfgFile: String): Option[Configuration] = {
    val cfgInfosOpt = FileSupport.readFile2kv(cfgFile, "=", '#')
    cfgInfosOpt match {
      case None => None
      case Some(cfgInfos) => this.getHbaseConfig(cfgInfos)
    }
  }
}