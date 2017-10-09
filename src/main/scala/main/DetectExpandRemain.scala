package main

import geotrellis.raster.{ArrayMultibandTile, DoubleArrayTile, FloatCellType}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import org.nd4j.linalg.factory.Nd4j
import org.nd4s.Implicits._
import geotrellis.raster._

object DetectExpandRemain {
  def detectExpandRemain (refCloud : String, refWater: String, targetCloud: String, targetWater: String, expand: String, remain: String): Unit = {
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
    var maskDataExpand = Array.ofDim[Double](Xsize*Ysize)
    var maskDataRemain = Array.ofDim[Double](Xsize*Ysize)
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
    val tifExpand = DoubleArrayTile(maskDataExpand, Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(tifExpand, geoSingleTiff.extent, geoSingleTiff.crs).write(expand)
    val tifRemain = DoubleArrayTile(maskDataRemain, Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(tifRemain, geoSingleTiff.extent, geoSingleTiff.crs).write(remain)
  }
}
