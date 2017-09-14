package main

import geotrellis.raster.{DoubleArrayTile, FloatCellType}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import org.apache.spark.SparkContext
import org.nd4j.linalg.factory.Nd4j
import org.nd4s.Implicits._

object DetectExpandRemain {
  def detectExpandRemain (target25: String, refCloud : String, refWater: String, targetCloud: String, targetWater: String): Array[Array[Array[Double]]] = {



    //get geo data Single Band
    val geoSingleTiff:SinglebandGeoTiff = GeoTiffReader.readSingleband(refCloud)

    //get data from 4 tiff images
    val refCloudArr = Utilities.Open1BandTif(refCloud)
    val refWaterArr = Utilities.Open1BandTif(refWater)
    val targetCloudArr = Utilities.Open1BandTif(targetCloud)
    val targetWaterArr = Utilities.Open1BandTif(targetWater)

    //get rows and cols
    val Xsize = Utilities.getColSingleBand(targetWater)
    val Ysize = Utilities.getRowSingleBand(targetWater)

    //create mask data
    var maskDataExpand = Nd4j.zeros(Xsize*Ysize)
    var maskDataRemain = Nd4j.zeros(Xsize*Ysize)
    val imgSize = Ysize*Xsize
    println("imgsize")
    println(imgSize)


    for (index <- 0 until imgSize) {
      if (targetWaterArr(index) >= 0) {
        if (targetCloudArr(index) != 1  && targetWaterArr(index) != 0 && refCloudArr(index) != 1) {
          if (refWaterArr(index) == 0) {
            maskDataExpand(index) = 1
            maskDataRemain(index) = 0
          }
          else {
            maskDataExpand(index) = 0
            maskDataRemain(index) = 1
          }
        }
        else {
          maskDataExpand(index) = 0
          maskDataRemain(index) = 0
        }
      }
    }
    var count, count2 = 0
    for (i <- 0 until imgSize) {
      if (maskDataExpand(i) != 1) count = count+1
      if (maskDataRemain(i) != 1) count2 = count2+1
    }
    println(count)
    println(count2)

    //--------------tron anh mat na va anh mau ----------------------------
    val geoTiffMul: MultibandGeoTiff = GeoTiffReader.readMultiband(target25)
    var counte,countr = 0
    var expandDataBand:Array[Array[Double]] = Utilities.Open4BandTif(target25)
    val remainDataBand:Array[Array[Double]] = Utilities.Open4BandTif(target25)
    for (bIndex <- 0 to 3){
      for (index <- 0 until Ysize*Xsize) {
        if (maskDataExpand(index) != 1.0) {
          expandDataBand(bIndex)(index) = Double.NaN
          counte = counte + 1
        }
        if (maskDataRemain(index) != 1.0) {
          remainDataBand(bIndex)(index) = Double.NaN
          countr = countr + 1
        }
      }
    }
    println("check")
    println(counte/4)
    println(countr/4)
    val ArrayResult: Array[Array[Array[Double]]] = Array.ofDim(2)
    ArrayResult(0) = expandDataBand
    ArrayResult(1) = remainDataBand
    ArrayResult
    // create mixed image from mask image and colorful image distributed
//    CreateExpandRemainImg.createExpandRemaining(maskDataExpand, maskDataRemain,target25, sc, expand, remain)
  }
}
