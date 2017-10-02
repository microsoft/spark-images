// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

scalaVersion := "2.11.8"

sparkVersion := "2.2.0"

spName := "microsoft/spark-images"

// Don't forget to set the version
version := "0.1"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))


// Add Spark components this package depends on, e.g, "mllib", ....
sparkComponents ++= Seq("sql")

libraryDependencies ++= Seq(
  // "%%" for scala things, "%" for plain java things
  "org.scalatest"    %% "scalatest"   % "3.0.0"  % "provided"
)

// uncomment and change the value below to change the directory where your zip artifact will be created
// spDistDirectory := target.value

// add any Spark Package dependencies using spDependencies.
// e.g. spDependencies += "databricks/spark-avro:0.1"
