package org.apache.spark.image

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

import java.awt.image.BufferedImage
import java.awt.{Color, Image}
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

object ImageSchema{

  val ocvTypes = Map(
    "CV_8U" -> 0, "CV_8UC1" -> 0, "CV_8UC2" -> 8, "CV_8UC3" -> 16, "CV_8UC4" -> 24,
    "CV_8S" -> 1, "CV_8SC1" -> 1, "CV_8SC2" -> 9, "CV_8SC3" -> 17, "CV_8SC4" -> 25,
    "CV_16U"-> 2, "CV_16UC1"-> 2, "CV_16UC2"->10, "CV_16UC3"-> 18, "CV_16UC4"-> 26,
    "CV_16S"-> 3, "CV_16SC1"-> 3, "CV_16SC2"->11, "CV_16SC3"-> 19, "CV_16SC4"-> 27,
    "CV_32S"-> 4, "CV_32SC1"-> 4, "CV_32SC2"->12, "CV_32SC3"-> 20, "CV_32SC4"-> 28,
    "CV_32F"-> 5, "CV_32FC1"-> 5, "CV_32FC2"->13, "CV_32FC3"-> 21, "CV_32FC4"-> 29,
    "CV_64F"-> 6, "CV_64FC1"-> 6, "CV_64FC2"->14, "CV_64FC3"-> 22, "CV_64FC4"-> 30
  )

  /** Schema for the image column: Row(String, Int, Int, Int, Array[Byte]) */
  val columnSchema = StructType(
    StructField("origin", StringType,  true) ::
    StructField("height", IntegerType, false) ::
    StructField("width",  IntegerType, false) ::
    StructField("nChannels",  IntegerType, false) ::
    StructField("mode", StringType, false) ::        //OpenCV-compatible type: CV_8U in most cases
    StructField("data",  BinaryType, false) :: Nil)   //bytes in OpenCV-compatible order: row-wise BGR in most cases

  //dataframe with a single column of images named "image" (nullable)
  private val imageDFSchema = StructType(StructField("image", columnSchema, true) :: Nil)

  def getOrigin(row: Row): String = row.getString(0)
  def getHeight(row: Row): Int = row.getInt(1)
  def getWidth(row: Row): Int = row.getInt(2)
  def getNChannels(row: Row): Int = row.getInt(3)
  def getMode(row: Row): String = row.getString(4)
  def getData(row: Row): Array[Byte] = row.getAs[Array[Byte]](5)

  /** Check if the dataframe column contains images (i.e. has ImageSchema)
    *
    * @param df
    * @param column
    * @return
    */
  def isImage(df: DataFrame, column: String): Boolean =
    df.schema(column).dataType == columnSchema


  /** Convert the image from compressd (jpeg, etc.) into OpenCV representation and store it in Row
    *
    * @param origin arbitrary string
    * @param bytes image bytes (for example, jpeg)
    * @return returns None if decompression fails
    */
  private[spark] def decode(origin: String, bytes: Array[Byte]): Option[Row] = {

    val img = ImageIO.read(new ByteArrayInputStream(bytes))

    if (img == null) {
      None
    } else {

      val height = img.getHeight
      val width = img.getWidth
      val (nChannels, mode) = if(img.getColorModel().hasAlpha()) (4, "CV_8UC4") else (3, "CV_8UC3") //TODO: grayscale

      assert(height*width*nChannels < 1e9, "image is too large")
      val decoded = Array.ofDim[Byte](height*width*nChannels)

      var offset = 0
      for(h <- 0 until height) {
        for (w <- 0 until width) {
          val color = new Color(img.getRGB(w, h))

          decoded(offset) = color.getBlue.toByte
          decoded(offset+1) = color.getGreen.toByte
          decoded(offset+2) = color.getRed.toByte
          if(nChannels == 4){
            decoded(offset+3) = color.getAlpha.toByte
          }
          offset += nChannels
        }
      }

      Some(Row(Row(origin, height, width, nChannels, mode, decoded)))
    }
  }


  /** Read the directory of images from the local or remote source
    *
    * @param path           Path to the image directory
    * @param recursive      Recursive path search flag
    * @param numPartitions  Number of dataframe partitions
    * @param sampleRatio    Fraction of the files loaded
    * @return               Dataframe with a single column "image" of images; see ImageSchema for details
    */
  def readImages(path: String, recursive: Boolean, numPartitions: Int = 0,
                 sampleRatio: Double = 1): DataFrame =   {
    require(sampleRatio <= 1.0 && sampleRatio >= 0, "sampleRatio should be between 0 and 1")

    val session = SparkSession.builder().getOrCreate
    val partitions = if(numPartitions > 0) numPartitions else session.sparkContext.defaultParallelism

    val oldRecursiveFlag = RecursiveFlag.setRecursiveFlag(Some(recursive.toString), session)
    val oldPathFilter: Option[Class[_]] =
      if (sampleRatio < 1)
        SamplePathFilter.setPathFilter(Some(classOf[SamplePathFilter]), Some(sampleRatio), session)
      else
        None

    var result: DataFrame = null
    try {
      val streams = session.sparkContext.binaryFiles(path, partitions)

      val images = streams.flatMap { row: (String, PortableDataStream) =>
        decode(row._1, row._2.toArray)
      }

      result = session.createDataFrame(images, imageDFSchema)
    }
    finally {
      // return Hadoop flags to the original values
      RecursiveFlag.setRecursiveFlag(oldRecursiveFlag, session)
      SamplePathFilter.setPathFilter(oldPathFilter, None, session)
      ()
    }

    result
  }
}
