package main

import geotrellis.raster.{DoubleArrayTile, FloatCellType, Tile}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import geotrellis.spark.io.hadoop._

import scala.collection.mutable.ArrayBuffer

object Utilities {
  def Max (B1:Double, B2: Double, B3:Double, B4: Double):Double = {
    var max = B1
    if (max < B2) max = B2
    if (max < B3) max = B3
    if (max < B4) max = B4
    max
  }
  def Open4BandTif (mulBandImg:String):Array[Array[Double]] = {
    var IndexArr: Array[Array[Double]] = Array(Array(), Array(), Array(), Array())
    //    val data:Array[Double] =
    val geoTiffMulBandImg:MultibandGeoTiff = GeoTiffReader.readMultiband(mulBandImg)
    for ( band <- 0 to 3 ){
      IndexArr(band) = geoTiffMulBandImg.raster.band(band).toArrayDouble()
    }
    //return
    IndexArr
  }
  def Open1BandTif (singleBandImg: String): Array[Double] ={
    var IndexArr: Array[Double] = Array()
    //    val data:Array[Double] =
    val geoTiffSingleBandImg:SinglebandGeoTiff = GeoTiffReader.readSingleband(singleBandImg)
    IndexArr = geoTiffSingleBandImg.raster.toArrayDouble()
    //return
    IndexArr
  }
  def getRowMultiBand (mulBandImg:String): Int = {
    val geoTiffMulBandImg:MultibandGeoTiff = GeoTiffReader.readMultiband(mulBandImg)
    var row: Int = geoTiffMulBandImg.tile.rows
    row
  }
  def getColMultiBand (mulBandImg:String): Int = {
    val geoTiffSingleBandImg:SinglebandGeoTiff = GeoTiffReader.readSingleband(mulBandImg)
    var col: Int = geoTiffSingleBandImg.tile.cols
    col
  }
  def getRowSingleBand (singleBandImg:String): Int = {
    val geoTiffSingleBandImg:SinglebandGeoTiff = GeoTiffReader.readSingleband(singleBandImg)
    var row: Int = geoTiffSingleBandImg.tile.rows
    row
  }
  def getColSingleBand (singleBandImg:String): Int = {
    val geoTiffMulBandImg:MultibandGeoTiff = GeoTiffReader.readMultiband(singleBandImg)
    var col: Int = geoTiffMulBandImg.tile.cols
    col
  }

  def NDIndex (B1:Double, B2: Double): Float = {
    ((B1-B2)/(B1+B2)).toFloat
  }

  def Min (B1:Double, B2: Double, B3:Double, B4: Double):Double = {
    var min = B1
    if (B2 < min) min = B2
    if (B3 < min) min = B3
    if (B4 < min) min = B4
    min
  }
  def Brightness (B1:Double, B2: Double, B3:Double, B4: Double):Float = {
    ((B1 + B2 + B3 + B4)/4).toFloat
  }
  def waterIndex (B0: Double, B3: Double) : Float = {
    ((B0 - B3)/(B0 + B3)).toFloat
  }

  //commit 2 RDD
  def readGeoTiff(sc: SparkContext, source: String): RDD[(ProjectedExtent, Tile)] = {
    // Read the geotiff in as a single image RDD,
    // using a method implicitly added to SparkContext by
    // an implicit class available via the
    // "import geotrellis.spark.io.hadoop._ " statement.
    sc.hadoopGeoTiffRDD(source)
  }
  def setMaskCloudName(name:String): String = {
    val maskCloudDir = "C:\\data\\Cloud_mask\\"
    maskCloudDir + name + "_Cloud" + ".tif"
  }
  def setMaskWaterName(name:String): String = {
    val maskWaterDir = "C:\\data\\Water_mask\\"
    maskWaterDir + name + "_Water" + ".tif"
  }
  def setMaskFeatureName(name:String, featureName: String): String = {
    val maskNdviDir = "C:\\data\\Feature\\"
    maskNdviDir + name + featureName + ".tif"
  }
  def setMaskExpandName(name:String): String = {
    val maskExpandDir = "C:\\data\\Expand\\"
    maskExpandDir + name + "_Expand" + ".tif"
  }
  def setMaskRemainName(name:String): String = {
    val maskRemainDir = "C:\\data\\Remain\\"
    maskRemainDir + name + "_Remain" + ".tif"
  }
  def setMaskDifferenceName(name: String, featureName: String) : String = {
    val maskNdviDir = "C:\\data\\Difference\\"
    maskNdviDir + name + featureName + ".tif"
  }
  def setMaskChangeName(name: String, changeName: String) : String = {
    val maskNdviDir = "C:\\data\\Change\\"
    maskNdviDir + name + changeName + ".tif"
  }
  def comparison (a: Double) : Boolean = {
    if (a != 1) true else false
  }
  def doubleArr2Tile (arr: Array[Double], Xsize: Int, Ysize:Int): Tile = {
    DoubleArrayTile(arr, Xsize, Ysize).convert(FloatCellType)
  }
  def mean (arr: ArrayBuffer[Double]) : Double = {
    val length: Int = arr.length
    var sum:Double = 0.0
    for (index <- 0 until length){
      sum = sum + arr(index)
    }
    sum/length
  }
  def std (arr: ArrayBuffer[Double]) : Double = {
    val length: Int = arr.length
    val meanIndex:Double = mean(arr)
    var devs:Double = 0.0
    for (index <- 0 until  length) {
      devs = devs + (arr(index) - meanIndex)*(arr(index) - meanIndex)
    }
    Math.sqrt(devs/(length-1))
  }
}
