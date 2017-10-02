import pyspark
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.types import Row, _create_row
from pyspark.sql import DataFrame
from pyspark.ml.param.shared import *
import numpy as np

undefinedImageType = "Undefined"

ImageFields = ["origin", "height", "width", "nChannels", "mode", "data"]

ocvTypes = {
    undefinedImageType : -1,
    "CV_8U" : 0, "CV_8UC1" : 0, "CV_8UC2" : 8, "CV_8UC3" : 16, "CV_8UC4" : 24,
    "CV_8S" : 1, "CV_8SC1" : 1, "CV_8SC2" : 9, "CV_8SC3" : 17, "CV_8SC4" : 25,
    "CV_16U": 2, "CV_16UC1": 2, "CV_16UC2":10, "CV_16UC3": 18, "CV_16UC4": 26,
    "CV_16S": 3, "CV_16SC1": 3, "CV_16SC2":11, "CV_16SC3": 19, "CV_16SC4": 27,
    "CV_32S": 4, "CV_32SC1": 4, "CV_32SC2":12, "CV_32SC3": 20, "CV_32SC4": 28,
    "CV_32F": 5, "CV_32FC1": 5, "CV_32FC2":13, "CV_32FC3": 21, "CV_32FC4": 29,
    "CV_64F": 6, "CV_64FC1": 6, "CV_64FC2":14, "CV_64FC3": 22, "CV_64FC4": 30
}

ImageSchema = StructType([
    StructField(ImageFields[0], StringType(),  True),
    StructField(ImageFields[1], IntegerType(), False),
    StructField(ImageFields[2], IntegerType(), False),
    StructField(ImageFields[3], IntegerType(), False),
    StructField(ImageFields[4], StringType(), False),     # OpenCV-compatible type: CV_8UC3 in most cases
    StructField(ImageFields[5], BinaryType(), False) ])   # bytes in OpenCV-compatible order: row-wise BGR in most cases

# TODO: generalize to other datatypes and number of channels
def toNDArray(image):
    """
    Converts an image to a 1-dimensional array

    Args:
        image (object): The image to be converted

    Returns:
        array: The image as a 1-dimensional array
    """
    return np.asarray(image.data, dtype = np.uint8).reshape((image.height, image.width, 3))[:,:,(2,1,0)]

# TODO: generalize to other datatypes and number of channels
def toImage(array, origin = "", mode = "CV_8UC3"):
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
    nChannels = array.shape[2]
    # Creating new Row with _create_row(), because Row(name = value, ... ) orders fields by name,
    # which conflicts with expected ImageSchema order when the new DataFrame is created by UDF
    return  _create_row(ImageFields, [origin, height, width, nChannels, mode, data])

def readImages(path,
               sparkSession = None,         #TODO: do we need this parameter in Python signature?
               recursive = False,
               numPartitions = 0,
               dropImageFailures = False,
               sampleRatio = 1.0):
    """
    Reads the directory of images from the local or remote (WASB) source.
    Example: spark.readImages(path, recursive = True)
    Args:
        path (str): Path to the image directory
        sparkSession (SparkSession): Existing sparkSession
        recursive (bool): Recursive search flag
        numPartitions (int): Number of the dataframe partitions
        dropImageFailures (bool): Drop the files that are not valid images from the result
        sampleRatio (double): Fraction of the images loaded
    Returns:
        DataFrame: DataFrame with a single column of "images", see imageSchema
        for details
    """
    ctx = SparkContext.getOrCreate()
    imageSchema = ctx._jvm.org.apache.spark.image.ImageSchema
    sql_ctx = pyspark.SQLContext.getOrCreate(ctx)
    jsession = sql_ctx.sparkSession._jsparkSession
    jresult = imageSchema.readImages(path, jsession, recursive, numPartitions, dropImageFailures, float(sampleRatio))
    return DataFrame(jresult, sql_ctx)
