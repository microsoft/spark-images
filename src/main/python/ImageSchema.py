# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import pyspark
from pyspark.sql.types import *
from pyspark.sql.types import Row, _create_row
import numpy as np

ImageFields = ["origin", "height", "width", "mode", "data"]

ImageSchema = StructType([
    StructField(ImageFields[0], StringType(),  True),
    StructField(ImageFields[1], IntegerType(), True),
    StructField(ImageFields[2], IntegerType(), True),
    StructField(ImageFields[3], IntegerType(), True),    # OpenCV-compatible type: CV_8U in most cases
    StructField(ImageFields[4], BinaryType(), True) ])   # bytes in OpenCV-compatible order: row-wise BGR in most cases

def toNDArray(image):
    """
    Converts an image to a 1-dimensional array

    Args:
        image (object): The image to be converted

    Returns:
        array: The image as a 1-dimensional array
    """
    return np.asarray(image.data, dtype = np.uint8).reshape((image.height, image.width, 3))[:,:,(2,1,0)]

def toImage(array, origin = "", mode = 16):
    """

    Converts a one-dimensional array to a 2 dimensional image

    Args:
        array (array):
        origin (str):
        mode (int):

    Returns:
        object: 2 dimensional image
    """
    length = np.prod(array.shape)

    data = bytearray(array.astype(dtype=np.int8)[:,:,(2,1,0)].reshape(length))
    height = array.shape[0]
    width = array.shape[1]
    # Creating new Row with _create_row(), because Row(name = value, ... ) orders fields by name,
    # which conflicts with expected ImageSchema order when the new DataFrame is created by UDF
    return  _create_row(ImageFields, [origin, height, width, mode, data])