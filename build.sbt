name := "hapi"

version := "1.0"

scalaVersion := "2.10.5"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.6.2",
  "org.apache.hbase" % "hbase" % "1.1.1",
  "org.apache.hbase" % "hbase-protocol" % "1.1.1",
  "org.apache.hbase" % "hbase-server" % "1.1.1",
  "org.apache.hbase" % "hbase-client" % "1.1.1",
  "org.apache.hbase" % "hbase-common" % "1.1.1",
  "org.json4s" % "json4s-jackson_2.10" % "3.3.0"
)

doc in Compile <<= target.map(_ / "none")