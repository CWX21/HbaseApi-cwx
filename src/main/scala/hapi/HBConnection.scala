/*
 * Copyright (c) 2015, BoDao, Inc. All Rights Reserved.
 *
 * Author@ dgl
 *
 * 本程序提供提供连接hbase的类
 * 可以使用kv键值对进行属性设置
 * 可以使用kv类型文件
 * 可以使用线程池多hbase进行操作
 */

package hapi

import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Admin}


class HBConnection {
  private var connect: Option[Connection] = None
  private var admin: Option[Admin] = None
  private var executors: Option[ExecutorService] = None

  def this(cfgs: Map[String, String]) {
    this()
    val hClusterConfig = new HClusterConfig()
    val cnt = ConnectionFactory.createConnection(hClusterConfig.getHbaseConfig(cfgs).get)
    this.connect = Some(cnt)
    this.admin = Some(cnt.getAdmin)
  }

  def this(fileName: String) {
    this()
    val hClusterConfig = new HClusterConfig()
    val cfg = hClusterConfig.getHbaseConfig(fileName)
    cfg match {
      case Some(hcfg) =>
        val cnt = ConnectionFactory.createConnection(hcfg)
        this.connect = Some(cnt)
        this.admin = Some(cnt.getAdmin)
      case None =>
        throw new Exception(s"read $fileName fail, can create a connect")
    }
  }

  /**
    * 使用线程池的操作
    * @param cfg 键值对的配置
    * @param executorAmount 线程数
    */
  def this(cfg: Map[String, String], executorAmount: Int) {
    this()
    val executors = Executors.newFixedThreadPool(executorAmount)
    val hClusterConfig = new HClusterConfig()
    val cnt = ConnectionFactory.createConnection(hClusterConfig.getHbaseConfig(cfg).get, executors)
    this.connect = Some(cnt)
    this.admin = Some(cnt.getAdmin)
    this.executors = Some(executors)
  }

  def this(fileName: String, executorAmount: Int) {
    this()
    val executors = Executors.newFixedThreadPool(executorAmount)
    this.executors = Some(executors)
    val hClusterConfig = new HClusterConfig()
    val cfg = hClusterConfig.getHbaseConfig(fileName)
    cfg match {
      case Some(hcfg) =>
        val cnt = ConnectionFactory.createConnection(hcfg, executors)
        this.connect = Some(cnt)
        this.admin = Some(cnt.getAdmin)
      case None =>
        throw new Exception(s"read $fileName fail, can create a connect")
    }
  }

  def getAdmin: Option[Admin] = {
    this.admin
  }

  def getHBConnection: Option[Connection] = {
    this.connect
  }

  def getExecutors: Option[ExecutorService] = {
    this.executors
  }
}

object HbaseTool {
  var connect: Option[HBConnection] = None

  def apply() = {
    if (!connectionInited()) connect = Some(new HBConnection())
  }

  def apply(cfgs: Map[String, String]) = {
    if (!connectionInited()) connect = Some(new HBConnection(cfgs))
  }

  def apply(fileName: String) = {
    if (!connectionInited()) {
      try {
        connect = Some(new HBConnection(fileName))
      } catch {
        case ex: Exception =>
          connect = None
          throw new Exception(s"init connection fail\n${ex.getStackTraceString}")
      }
    }
  }

  def apply(cfgs: Map[String, String], executorAmount: Int) = {
    if (!connectionInited()) connect = Some(new HBConnection(cfgs, executorAmount))
  }

  def apply(fileName: String, executorAmount: Int) = {
    if (!connectionInited()) {
      try {
        connect = Some(new HBConnection(fileName, executorAmount))
      } catch {
        case ex: Exception => connect = None
      }
    }
  }

  private def connectionInited(): Boolean = {
    connect match {
      case None => false
      case Some(c) => true
    }
  }

  def getHbaseConnection: HBConnection = {
    if (connectionInited()) connect.get
    else {
      apply()
      connect.get
    }
  }

  def getConnection: Connection = {
    if (connectionInited()) {
      connect.get.getHBConnection.get
    }else {
      apply()
      connect.get.getHBConnection.get
    }
  }

  def getAdmin: Admin = {
    if (connectionInited()) {
      connect.get.getAdmin.get
    }else {
      apply()
      connect.get.getAdmin.get
    }
  }

  def getExecutors: Option[ExecutorService] = {
    if (connectionInited()) {
      connect.get.getExecutors
    } else None
  }

  def closeConnect() = {
    connect match {
      case Some(hbConnection) =>
        hbConnection.getAdmin match {
          case None =>
          case Some(admin) => admin.close()
        }

        hbConnection.getHBConnection match {
          case None =>
          case Some(cnt) => if (!cnt.isClosed) cnt.close()
        }

        hbConnection.getExecutors match {
          case None =>
          case Some(exec) => exec.shutdown()
        }

      case None =>
    }
  }
}