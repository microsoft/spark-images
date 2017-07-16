// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.image

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

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
    StructField("mode", IntegerType, false) ::        //OpenCV-compatible type: CV_8U in most cases
    StructField("data",  BinaryType, false) :: Nil)   //bytes in OpenCV-compatible order: row-wise BGR in most cases

  def getOrigin(row: Row): String = row.getString(0)
  def getHeight(row: Row): Int = row.getInt(1)
  def getWidth(row: Row): Int = row.getInt(2)
  def getMode(row: Row): Int = row.getInt(3)
  def getData(row: Row): Array[Byte] = row.getAs[Array[Byte]](4)

  /** Check if the dataframe column contains images (i.e. has ImageSchema)
    *
    * @param df
    * @param column
    * @return
    */
  def isImage(df: DataFrame, column: String): Boolean =
    df.schema(column).dataType == columnSchema
}
