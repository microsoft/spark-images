package org.apache.spark.image

import org.scalatest.FunSuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, DataFrame, SQLContext, SparkSession}
import org.apache.spark.rdd.RDD

class TestImageSchemaSuite extends FunSuite with TestSparkContext {

  //single column of images named "image"
  private val imageDFSchema = StructType(StructField("image", ImageSchema.columnSchema, true) :: Nil)

  test("Create basic Spark dataframe") {
    val df = spark.createDataFrame(Seq((0, 0.0)))
    assert(df.count == 1)
  }

  test("Create basic ImageSchema dataframe") {
    val origin = "path"
    val width = 1
    val height = 1
    val data = Array[Byte](0,0,0)
    val mode = ImageSchema.ocvTypes("CV_8UC3")

    val rows = Seq(Row(Row(origin, height, width, mode, data)))   //internal Row corresponds to image StructType
    val rdd = sc.makeRDD(rows)
    val df = spark.createDataFrame(rdd, imageDFSchema)

    df.printSchema

    assert(df.count == 1, "incorrect image count")
    assert(ImageSchema.isImage(df,"image"), "data do not fit ImageSchema")
  }

}