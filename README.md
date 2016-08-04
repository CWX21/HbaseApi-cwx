#hapi

###简介
HbaseApi是一个使用scala编写的调用hbase API的简单API库,提供简单的hbase管理、get、scan、put等接口。

###使用

####HbaseTool 负责初始化hbase的连接等操作
#####API
```
1、def apply() = {}
2、def apply(cfgs: MuMap[String, String]) = {}
3、def apply(fileName: String) = {}
4、def apply(cfgs: MuMap[String, String], executorAmount: Int) = {}
5、def apply(fileName: String, executorAmount: Int) = {}
6、def getHbaseConnection: HBConnection = {}
7、def getConnection: Connection = {}
8、def getAdmin: Admin = {}
9、def getExecutors: Option[ExecutorService] = {}
10、def closeConnect() = {}
```
#####例子
```
import hapi.HbaseTool

/* 利用kv键值对配置hbase的连接属性 */
HbaseTool.apply("./DycConfigs/HClusterConfig")

/* 如果成功, 可以获取admin、connection等 */
val admin = HbaseTool.getAdmin
```

####HManager 负责hbase的管理操作
#####API
```
1、def tableExist(admin: Admin, tb: String): Boolean = {}
2、def createTable(admin: Admin, tb: String, hFamilys: TraversableOnce[HFamily]): Boolean = {}
3、def createTable(admin: Admin, tb: String, hFamily: HFamily): Boolean = {}
4、def createTableForce(admin: Admin, tb: String, hFamilys: TraversableOnce[HFamily]): Boolean = {}
5、def createTableForce(admin: Admin, tb: String, hFamily: HFamily): Boolean = {}
6、def dropTable(admin: Admin, tb: String): Boolean = {}
7、def addFamily(admin: Admin, tb: String, hFamily: HFamily): Boolean = {}
8、def addFamilys(admin: Admin, tb: String, hFamilys: TraversableOnce[HFamily]): Boolean = {}
9、def deleteFamily(admin: Admin, tb: String, hFamily: HFamily): Boolean = {
10、def deleteFamilys(admin: Admin, tb: String, hFamilys: TraversableOnce[HFamily]): Boolean = {}
11、def familyExist(admin: Admin, tb: String, familyName: String): Boolean = {}
12、def updateFamilyArrts(admin: Admin, tb: String, hFamily: HFamily): Boolean = {}
13、def updateFamilyArrts(admin: Admin, tb: String, hFamilys: TraversableOnce[HFamily]): Boolean = {}
```
#####例子
```
import hapi.HbaseTool
import hapi.HManager

HbaseTool.apply("./DycConfigs/HClusterConfig")
val admin = HbaseTool.getAdmin

val hf = new HFamily("diu1")
hf.setMaxVersions(100)

if (!HManager.tableExist(admin, "testapi")) {
  val status = HManager.createTable(admin, "testapi", hf)
  if (!status) println(HBError.getAndCleanErrors())
  else println("创建表成功")
}

/* 加入列簇 */
val map = MuMap[String, String]()

/* 设置列簇属性 */
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
hf2.setHColumnDescriptorAttrs(map)

val arr = ArrayBuffer[HFamily]()
arr += (hf, hf1, hf2)

val status = HManager.addFamilys(admin, "testapi", arr)
if (!status) println(HBError.getAndCleanErrors())
else println("加入列簇成功")


/* 删除列簇 */
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
/* 其他例子请参考 test的代码 */

```

####HWriter 负责写入数据
#####API
```
1、def putOne(admin: Admin, cnt: Connection, tb: String, columnrow: HBColumnRow): Boolean = {}
2、def putRows(puter: HBRowPuter): Boolean = {}
3、def putRowsAtFamily(puter: HBFamilyPuter): Boolean = {}
```
#####例子
```
import hapi.HBColumn.{HBRowCell, HBColumnFamily, HBColumnRow}
import hapi.HWriter

/* 写入一个 */
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

/* 写入多个 */
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
puter.bufSize = 1024 * 1024 * 3
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
```

####HReader 负责读取hbase行、列的内容
#####API
```
1、def resolveResult(result: Result) = {}
2、def resolveResults(results: Array[Result]) = {}
3、def resolveResult2Json(result: Result): String = {}
4、def resolveResults2Json(results: Array[Result]): String = {}
5、def getRow(hbGeter: HBGeter, rowKey: String): Option[Result] = {}
6、def getRows(hbGeter: HBGeter, rowKeys: Traversable[String]): Option[Array[Result]] = {}
7、def getRowByQualifier(hbGeter: HBQualifierGeter, rowKey: String, qualifier: String): Option[Result] = {}
8、def getRowsByQualifier(hbGeter: HBQualifierGeter, rowKeys: Traversable[String],  qualifier: String): Option[Array[Result]] = {}
9、def getRowByQualifiers(hbGeter: HBQualifierGeter, rowKey: String, qualifiers: Traversable[String]): Option[Result] = {}
10、def getRowsByQualifiers(hbGeter: HBQualifierGeter, rowCells: MuMap[String, Traversable[String]]): Option[Array[Result]] = {}
11、def getRowByFamily(hbGeter: HBGeter, family: String, row: String): Option[Result] = {}
12、def getRowsByFamily(hbGeter: HBGeter, family: String, rows: Traversable[String]): Option[Array[Result]] = {}
13、def getRowByFamilys(hbGeter: HBGeter, familys: Traversable[String], row: String): Option[Result] = {}
14、def getRowsByFamilys(hbGeter: HBGeter, familys: Traversable[String], rows: Traversable[String]): Option[Array[Result]] = {}
```
####例子
```
import hapi.Geter.{HBGeter, HBQualifierGeter}
import hapi.Puter.{HBRowPuter, HBFamilyPuter}
import hapi.HReader
import hapi.HBColumn.{HBRowCell, HBColumnFamily, HBColumnRow}
import scala.collection.mutable.ArrayBuffer

/* 读一个 */
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

/* 读多个 */
HbaseTool.apply("./DycConfigs/HClusterConfig")
val cnt = HbaseTool.getHbaseConnection
val reader = new HReader()
val rowsQul = MuMap[String, ArrayBuffer[String]]()
val geter = new HBQualifierGeter(cnt, "testapi", "diu1")
val quls = ArrayBuffer[String]()
quls += "col1"
quls += "col2"
quls += "col3"

for (i <- 11 to 13) {
  rowsQul += (i.toString -> quls)
}

val hh = reader.getRowsByQualifiers(geter, rowsQul)

hh match {
  case None =>
    println(HBError.getAndCleanErrors())
  case Some(rs) =>
    reader.resolveResults(rs)
}
/* 其他例子请参考 test的代码*/
```

####HObtainer 是一个scan的封装
#####API
```
1、def closeScanner(scanResult: ResultScanner): Boolean = {}
2、def resolveScanResult(scanResult: ResultScanner, hbcs: Array[HBFamilyQualifier], func: (Iterator[Result], Array[HBFamilyQualifier]) => Any = printScanResult): Option[Any] = {}
3、def scan(hbScanner: HBScanner): Option[ResultScanner] = {}
```
#####例子
```
import hapi.Geter.{HBScanner, HBGeter, HBQualifierGeter}
import hapi.Puter.{HBRowPuter, HBFamilyPuter}
import hapi.HBScanner
import scala.collection.mutable.ArrayBuffer

HbaseTool.apply("./DycConfigs/HClusterConfig")
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
//    val geter = new HBGeter(cnt, "testapi")
//    val scanner = new HObtainer()
//    scanner.scan(geter)
```
