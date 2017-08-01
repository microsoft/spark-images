package org.apache.spark.image

import org.scalatest.FunSuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, DataFrame, SQLContext, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.image.ImageSchema._
import java.nio.file.Paths

class TestImageSchemaSuite extends FunSuite with TestSparkContext {

  //single column of images named "image"
  private val imageDFSchema = StructType(StructField("image", ImageSchema.columnSchema, true) :: Nil)
  private lazy val imagePath = getClass.getResource("/images").getPath

  test("Smoke test: create basic Spark dataframe") {
    val df = spark.createDataFrame(Seq((0, 0.0)))
    assert(df.count == 1)
  }

  test("Smoke test: create basic ImageSchema dataframe") {
    val origin = "path"
    val width = 1
    val height = 1
    val nChannels = 3
    val data = Array[Byte](0, 0, 0)
    val mode = "CV_8UC3"

    val rows = Seq(Row(Row(origin, height, width, nChannels, mode, data)), //internal Row corresponds to image StructType
      Row(Row(null, height, width, nChannels, mode, data)))
    val rdd = sc.makeRDD(rows)
    val df = spark.createDataFrame(rdd, imageDFSchema)

    assert(df.count == 2, "incorrect image count")
    assert(ImageSchema.isImage(df, "image"), "data do not fit ImageSchema")
  }

  test("readImages count test") {
    var df = readImages(imagePath, recursive = false)
    assert(df.count == 0)

    df = readImages(imagePath, recursive = true)
    assert(df.count == 104)
    df = df.na.drop(Seq("image.data"))    //dropping non-image files
    val count100 = df.count
    assert(count100 == 103)

    df = readImages(imagePath, recursive = true, sampleRatio = 0.5).na.drop(Seq("image.data"))
    val count50 = df.count //random number about half of the size of the original dataset
    assert(count50 > 0.2 * count100 && count50 < 0.8 * count100)
  }

  //images with the different number of channels
  test("readImages pixel values test") {

    val images = readImages(imagePath + "/multi-channel/", recursive = false).collect

    images.foreach{
      rrow => {
        val row = rrow.getAs[Row](0)
        val filename = Paths.get(getOrigin(row)).getFileName().toString()
        if(firstBytes20.contains(filename)) {
          val mode = getMode(row)
          val bytes20 = getData(row).slice(0, 20)

          val expectedMode = firstBytes20(filename)._1
          val expectedBytes = firstBytes20(filename)._2

          assert(expectedMode == mode, "mode of the image is not read correctly")

          if (!compareBytes(expectedBytes, bytes20)) {
            println(filename)
            println("result:   " + bytes20.deep.toString)
            println("expected: " + expectedBytes.deep.toString)
            throw new Exception("incorrect numeric value for flattened image")
          }
        }
      }
    }
  }

  // TODO: fix grayscale test
  // number of channels and first 20 bytes of OpenCV representation
  // - default representation for 3-channel RGB images is BGR row-wise:  (B00, G00, R00,      B10, G10, R10,      ...)
  // - default representation for 4-channel RGB images is BGRA row-wise: (B00, G00, R00, A00, B10, G10, R10, A00, ...)
  private val firstBytes20 = Map(
    //"grayscale.png" -> (("CV_8UC1", Array[Byte](0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 3, 5, 2, 1))),
    "RGB.png" -> (("CV_8UC3", Array[Byte](-34, -66, -98, -38, -69, -98, -62, -90, -117, -70, -98, -124, -34, -63, -90, -20, -48, -74, -18, -45))),
    "RGBA.png" -> (("CV_8UC4", Array[Byte](-128, -128, -8, -1, -128, -128, -8, -1, -128, -128, -8, -1, 127, 127, -9, -1, 127, 127, -9, -1)))
  )

  private def compareBytes(x: Array[Byte], y:Array[Byte]): Boolean = {
    val length = Math.min(x.length, y.length)
    for (i <- 0 to length-1) {
      if (x(i) != y(i)) return false
    }
    true
  }
}