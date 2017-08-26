val sparkVer = sys.props.getOrElse("spark.version", "2.1.1")
val sparkBranch = sparkVer.substring(0, 3)
val defaultScalaVer = sparkBranch match {
  case "2.0" => "2.11.8"
  case "2.1" => "2.11.8"
  case "2.2" => "2.11.8"
  case _ => throw new IllegalArgumentException(s"Unsupported Spark version: $sparkVer.")
}
val scalaVer = sys.props.getOrElse("scala.version", defaultScalaVer)

val sparkVersion = "2.1.1"
scalaVersion := scalaVer

name := "spark-image"
version := "0.1"

licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

// Add Spark components this package depends on, e.g, "mllib", ....
val sparkComponents = Seq("sql")

libraryDependencies ++= Seq(
  // "%%" for scala things, "%" for plain java things
  "com.typesafe" % "config" % "1.3.1",
  "org.apache.logging.log4j" %  "log4j-api"       % "2.8.1" % "provided",
  "org.apache.logging.log4j" %  "log4j-core"      % "2.8.1" % "provided",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "2.8.1" % "provided",
  "org.apache.spark" %% "spark-core"  % sparkVer % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVer % "provided",
  "org.scalatest"    %% "scalatest"   % "3.0.0"  % "provided"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

parallelExecution := false

// This fixes a class loader problem with scala.Tuple2 class, scala-2.11, Spark 2.x
fork in Test := true

// This and the next line fix a problem with forked run: https://github.com/scalatest/scalatest/issues/770
javaOptions in Test ++= Seq("-Xmx2048m", "-XX:ReservedCodeCacheSize=384m", "-XX:MaxPermSize=384m")

concurrentRestrictions in Global := Seq(
  Tags.limitAll(1))

autoAPIMappings := true
