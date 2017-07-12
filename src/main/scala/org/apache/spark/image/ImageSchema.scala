// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.image

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

object ImageSchema{

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
