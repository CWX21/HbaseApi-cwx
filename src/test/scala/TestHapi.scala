package Test

import hapi.Geter.{HBScanner, HBGeter, HBQualifierGeter}
import hapi.Puter.{HBRowPuter, HBFamilyPuter}
import hapi._
import hapi.HBColumn.{HBRowCell, HBColumnFamily, HBColumnRow}
import org.apache.hadoop.hbase.client.Durability
import scala.collection.mutable.{ArrayBuffer, Map => MuMap}

/**
  * Created by lele on 16-1-13.
  */
object TestHapi {

  private def testCreate() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig")
    val admin = HbaseTool.getAdmin

    val hf = new HFamily("diu1")
    hf.setMaxVersions(100)

    if (!HManager.tableExist(admin, "testapi")) {
      val status = HManager.createTable(admin, "testapi", hf)
      if (!status) println(HBError.getAndCleanErrors())
      else println("创建表成功")
    }

    val map = MuMap[String, String]()
    map += ("keepdeletecells" -> "true")  //true or false, string
    map += ("maxversions" -> "100")     //num
    map += ("minversions" -> "1")       //num
    map += ("ttl" -> "10000000")        //num
    map += ("blocksize" -> (1024 * 128).toString) //1k ~ 16M, num
    map += ("inmem" -> "true")          //true or false
    map += ("bloomfilter" -> "row")     //row or rowcol or don't set it defaule is None
    map += ("blockcache" -> "true")     //true or false
    map += ("scope" -> "1")             //0 or 1, num

    val hf1 = new HFamily("diu2")
    hf1.setMaxVersions(100)

    val hf2 = new HFamily("diu3")
    hf2.setHColumnDescriptorAttrs(map.toMap)

    val arr = ArrayBuffer[HFamily]()
    arr += (hf, hf1, hf2)

    val status = HManager.addFamilys(admin, "testapi", arr)
    if (!status) println(HBError.getAndCleanErrors())
    else println("加入列簇成功")
  }

  private def testdeleteFamily() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig")
    val admin = HbaseTool.getAdmin

    val hf = new HFamily("diu1")
    val hf1 = new HFamily("diu2")
    val hf2 = new HFamily("diu4")

    val arr = ArrayBuffer[HFamily]()
    arr += (hf, hf1, hf2)

    if (!HManager.tableExist(admin, "testapi")) {
      val status = HManager.createTable(admin, "testapi", hf)
      println(status)
    }

    val status = HManager.deleteFamilys(admin, "testapi", arr)
    if (!status) println(HBError.getAndCleanErrors())
    else println("删除列簇成功")
  }

  private def testUpdateFamily() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig")
    val admin = HbaseTool.getAdmin

    val map = MuMap[String, String]()
    map += ("keepdeletecells" -> "true")
    map += ("maxversions" -> "1100")
    map += ("minversions" -> "1")
    map += ("ttl" -> "10000000")
    map += ("blocksize" -> (1024 * 128).toString)
    map += ("inmem" -> "true")
    map += ("bloomfilter" -> "row")
    map += ("scope" -> "1")

    val hf = new HFamily("diu")
    hf.setHColumnDescriptorAttrs(map.toMap)

    val hf1 = new HFamily("diu3")
    hf1.setHColumnDescriptorAttrs(map.toMap)

    val arr = ArrayBuffer[HFamily]()
    arr += (hf, hf1)

    //    val status = HManager.updateFamilyArrts(admin, "testapi", hf)
    //    if (status == false) println(HBError.getAndCleanErrors())
    //    else println("更新列簇成功")

    val status = HManager.updateFamilyArrts(admin, "testapi", arr)
    if (!status) println(HBError.getAndCleanErrors())
    else println("更新列簇成功")
  }

  private def testWriteOne() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig")
    val admin = HbaseTool.getAdmin
    val connection = HbaseTool.getConnection
    val hbWriter = new HWriter()
    val hbRow = new HBColumnRow("10")  //创建一行
    val hbFamily = new HBColumnFamily("diu2") //加入一个family

    hbFamily.addColumnCell("col2_1", "test1") //加入列值
    hbFamily.addColumnCell("col2_2", "test2")
    hbRow.addFamily(hbFamily)

    val status = hbWriter.putOne(admin, connection, "testapi", hbRow)
    if (!status) {
      println("put 失败")
      println(HBError.getAndCleanErrors())
    }else {
      println("put 成功")
    }
  }

  private def testWriteMult1() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig", 4)
    val cnt = HbaseTool.getHbaseConnection
    val puter = new HBRowPuter(cnt, "testapi")
    val hbWriter = new HWriter()
    val rows = ArrayBuffer[HBColumnRow]()

    for (i <- 1 to 100000) {
      val hbRow = new HBColumnRow(i.toString)

      val hbFamily = new HBColumnFamily("diu1")
      hbFamily.addColumnCell("col1", "testtesttesttesttestesttesttesttesttesttesttesttesttesttesttestestt" + i.toString)
      hbFamily.addColumnCell("col2", "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttetest" + i.toString)
      hbFamily.addColumnCell("col3", "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttestest" + i.toString)
      hbRow.addFamily(hbFamily)

      val hbFamily1 = new HBColumnFamily("diu2")
      hbFamily1.addColumnCell("col2_1", "testtesttesttesttestesttesttesttesttesttesttesttesttesttesttestestt" + i.toString)
      hbFamily1.addColumnCell("col2_2", "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttetest" + i.toString)
      hbFamily1.addColumnCell("col2_3", "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttestest" + i.toString)
      hbRow.addFamily(hbFamily1)

      rows += hbRow
    }

    puter.data = rows
    puter.bufSize = 1024 * 1024 * 4
    puter.durability = Durability.USE_DEFAULT

    println("start")
    val s = System.currentTimeMillis
    /**
      * ASYNC_WAL ： 当数据变动时，异步写WAL日志
      * SYNC_WAL ： 当数据变动时，同步写WAL日志
      * FSYNC_WAL ： 当数据变动时，同步写WAL日志，并且，强制将数据写入磁盘
      * SKIP_WAL ： 不写WAL日志
      * USE_DEFAULT ： 使用HBase全局默认的WAL写入级别，即 SYNC_WAL
      */
    val status = hbWriter.putRows(puter)
    val e = System.currentTimeMillis
    if (!status) {
      println(HBError.getAndCleanErrors())
      println("批量插入失败")
    }else {
      println("批量插入成功")
    }
    println(e - s + " ms")
    println((e - s) / 1000 + " s")
    //    HbaseTool.closeConnect()
  }

  private def testWriteMultAtFamily() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig", 4)
    val hbWriter = new HWriter()
    val cnt = HbaseTool.getHbaseConnection

    val puter = new HBFamilyPuter(cnt, "testapi", "diu1")
    puter.bufSize = 1024 * 1024 * 1
    puter.durability = Durability.USE_DEFAULT

    val rows = ArrayBuffer[HBRowCell]()
    for (i <- 1 to 100000) {
      val rowCell = new HBRowCell(i.toString)
      rowCell.addRowCell("rcol1", "testtesttesttesttestesttesttesttesttesttesttesttesttesttesttestestt" + i.toString)
      rowCell.addRowCell("rcol2", "testtesttesttesttestesttesttesttesttesttesttesttesttesttesttestestt" + i.toString)
      rowCell.addRowCell("rcol3", "testtesttesttesttestesttesttesttesttesttesttesttesttesttesttestestt" + i.toString)
      rows += rowCell
    }

    puter.data = rows

    println("start")
    var s = System.currentTimeMillis
    var status = hbWriter.putRowsAtFamily(puter)
    var e = System.currentTimeMillis
    if (!status) {
      println(HBError.getAndCleanErrors())
      println("插入失败")
    }else {
      println("插入成功")
    }

    println(e - s + " ms")
    println((e - s) / 1000 + " s")

    println("start")
    s = System.currentTimeMillis
    status = hbWriter.putRowsAtFamily(puter)
    e = System.currentTimeMillis
    if (!status) {
      println(HBError.getAndCleanErrors())
      println("插入失败")
    }else {
      println("插入成功")
    }

    println(e - s + " ms")
    println((e - s) / 1000 + " s")

    println("start")
    s = System.currentTimeMillis
    status = hbWriter.putRowsAtFamily(puter)
    e = System.currentTimeMillis
    if (!status) {
      println(HBError.getAndCleanErrors())
      println("插入失败")
    }else {
      println("插入成功")
    }

    println(e - s + " ms")
    println((e - s) / 1000 + " s")

    println("start")
    s = System.currentTimeMillis
    status = hbWriter.putRowsAtFamily(puter)
    e = System.currentTimeMillis
    if (!status) {
      println(HBError.getAndCleanErrors())
      println("插入失败")
    }else {
      println("插入成功")
    }

    println(e - s + " ms")
    println((e - s) / 1000 + " s")
    HbaseTool.closeConnect()
  }

  private def testReaderOne() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig")
    val cnt = HbaseTool.getHbaseConnection
    val geter = new HBGeter(cnt, "testapi")
    geter.maxVersion = 2
    val reader = new HReader()
    val hh = reader.getRow(geter, "1")
    hh match {
      case None =>
        println(HBError.getAndCleanErrors())
      case Some(rs) =>
        //        reader.resolveResult(rs)
        val ret = reader.resolveResult2Json(rs)
        println(ret)
    }
  }

  private def testReaderMult() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig", 4)
    val cnt = HbaseTool.getHbaseConnection
    val geter = new HBGeter(cnt, "testapi")
    val reader = new HReader()
    val keys = ArrayBuffer[String]()
    for (i <- 1 to 30) {
      keys += i.toString
    }
    geter.maxVersion = 2
    println("start")
    val s = System.currentTimeMillis
    val hh = reader.getRows(geter, keys)
    val e = System.currentTimeMillis
    hh match {
      case None =>
        println(HBError.getAndCleanErrors())
      case Some(rs) =>
        reader.resolveResults(rs)
        val ret = reader.resolveResults2Json(rs)
        println(ret)
    }
    println(e - s + " ms")
    println((e - s) / 1000 + " s")
  }

  private def testGetRowByQualifier() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig", 4)
    val cnt = HbaseTool.getHbaseConnection
    val geter = new HBQualifierGeter(cnt, "testapi", "diu1")
    val reader = new HReader()
    val hh = reader.getRowByQualifier(geter,"10", "col1")
    hh match {
      case None =>
        println(HBError.getAndCleanErrors())
      case Some(rs) =>
        reader.resolveResult(rs)
    }
  }

  private def testGetRowByQualifiers() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig")
    val cnt = HbaseTool.getHbaseConnection
    val geter = new HBQualifierGeter(cnt, "testapi", "diu1")
    val reader = new HReader()
    val qul = ArrayBuffer[String]()
    qul += "col1"
    qul += "col2"
    qul += "col3"

    val hh = reader.getRowByQualifiers(geter, "10", qul)
    hh match {
      case None =>
        println(HBError.getAndCleanErrors())
      case Some(rs) =>
        reader.resolveResult(rs)
    }
  }

  private def testGetRowsByQualifier() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig")
    val cnt = HbaseTool.getHbaseConnection
    val geter = new HBQualifierGeter(cnt, "testapi", "diu1")
    val reader = new HReader()
    val keys = ArrayBuffer[String]()
    keys += "1"
    keys += "11"
    keys += "12"

    val hh = reader.getRowsByQualifier(geter, keys, "col1")
    hh match {
      case None =>
        println(HBError.getAndCleanErrors())
      case Some(rs) =>
        reader.resolveResults(rs)
    }
  }

  private def testGetRowsByQualifiers() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig")
    val cnt = HbaseTool.getHbaseConnection
    val reader = new HReader()
    val rowsQul = MuMap[String, Traversable[String]]()
    val geter = new HBQualifierGeter(cnt, "testapi", "diu1")
    val quls = ArrayBuffer[String]()
    quls += "col1"
    quls += "col2"
    quls += "col3"

    for (i <- 11 to 13) {
      rowsQul += (i.toString -> quls)
    }

    val hh = reader.getRowsByQualifiers(geter, rowsQul.toMap)

    hh match {
      case None =>
        println(HBError.getAndCleanErrors())
      case Some(rs) =>
        reader.resolveResults(rs)
    }
  }

  private def testGetRowByFamily() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig")
    val cnt = HbaseTool.getHbaseConnection
    val geter = new HBGeter(cnt, "testapi")
    val reader = new HReader()

    //    geter.maxVersion = 1
    val hh = reader.getRowByFamily(geter, "diu1", "10")
    hh match {
      case None =>
        println(HBError.getAndCleanErrors())
      case Some(rs) =>
        reader.resolveResult(rs)
    }
  }

  private def testGetRowsByFamily() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig")
    val cnt = HbaseTool.getHbaseConnection
    val geter = new HBGeter(cnt, "testapi")
    val reader = new HReader()
    val rows = ArrayBuffer[String]()
    rows += "10"
    rows += "11"
    rows += "12"
    //    geter.maxVersion = 1
    val hh = reader.getRowsByFamily(geter, "diu1", rows)
    hh match {
      case None =>
        println(HBError.getAndCleanErrors())
      case Some(rs) =>
        reader.resolveResults(rs)
    }
  }

  private def testGetRowByFamilys() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig")
    val cnt = HbaseTool.getHbaseConnection
    val geter = new HBGeter(cnt, "testapi")
    val reader = new HReader()
    //    geter.maxVersion = 1
    val familys = ArrayBuffer[String]()
    familys += "diu1"
    familys += "diu2"

    val hh = reader.getRowByFamilys(geter, familys, "10")
    hh match {
      case None =>
        println(HBError.getAndCleanErrors())
      case Some(rs) =>
        reader.resolveResult(rs)
    }
  }

  private def testGetRowsByFamilys() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig")
    val cnt = HbaseTool.getHbaseConnection
    val geter = new HBGeter(cnt, "testapi")
    val reader = new HReader()
    val familys = ArrayBuffer[String]()
    familys += "diu1"
    familys += "diu5"
    //    geter.maxVersion = 1
    val rows = ArrayBuffer[String]()
    rows += "10"
    rows += "11"
    rows += "12"

    val hh = reader.getRowsByFamilys(geter, familys, rows)
    hh match {
      case None =>
        println(HBError.getAndCleanErrors())
      case Some(rs) =>
        reader.resolveResults(rs)
    }
  }

  private def testScanner() = {
    HbaseTool.apply("./DycConfigs/HClusterConfig", 4)
    val cnt = HbaseTool.getHbaseConnection
    val hbScanner = new HBScanner(cnt, "testapi")
    hbScanner.setStartRow("1".getBytes())
    hbScanner.setStopRow("11".getBytes())
    val hObt = new HObtainer()
    val rs = hObt.scan(hbScanner)

    rs match {
      case None =>
        println(HBError.getAndCleanErrors())
        println("scan 数据失败")
      case Some(result) =>
        hObt.resolveScanResult(result, Array(new HBFamilyQualifier("diu1".getBytes(), "col1".getBytes())))
        hObt.closeScanner(result)
    }

    HbaseTool.closeConnect()
    //    val geter = new HBGeter(cnt, "testapi")
    //    val scanner = new HObtainer()
    //    scanner.scan(geter)
  }

  def main(args: Array[String]) {
    try {
      //      testWriteMult1()
      //      testWriteOne()
      //      val hh: String = null
      //      if (null == hh) println("======")
      //      println(Unit.toString().getBytes())
      //      testCreate()
      //      testdeleteFamily()
      //      testUpdateFamily()
      //      testWriteMultAtFamily()
      //      testReaderOne()
      //      testReaderMult()
      //      testGetRowByQualifier()
      //      testGetRowByQualifiers()
      //      testGetRowsByQualifier()
      //      println("---------")
      //      testGetRowsByQualifiers()
      //      testGetRowByFamily()
      //      testGetRowsByFamily()
      //      testGetRowByFamilys()
      //      testGetRowsByFamilys()
      testScanner()
    } catch {
      case ex: Exception => println("ee" + ex.getMessage)
    }
  }
}
