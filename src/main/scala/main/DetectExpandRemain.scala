package main

import geotrellis.raster.{DoubleArrayTile, FloatCellType}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import org.nd4j.linalg.factory.Nd4j
import org.nd4s.Implicits._

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
    var maskDataExpand = Nd4j.zeros(Xsize*Ysize)
    var maskDataRemain = Nd4j.zeros(Xsize*Ysize)
    val imgSize = Ysize*Xsize

    for (index <- 0 until imgSize) {
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
    var expandTiffMask = Array.ofDim[Double](imgSize)
    var remainTiffMask = Array.ofDim[Double](imgSize)
    for (index <- 0 until imgSize){
      expandTiffMask(index) = maskDataExpand(index)
      remainTiffMask(index) = maskDataRemain(index)
    }
    val expandTiff = DoubleArrayTile(expandTiffMask, Xsize, Ysize).convert(FloatCellType)
    val remainTiff = DoubleArrayTile(remainTiffMask, Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(expandTiff, geoSingleTiff.extent, geoSingleTiff.crs).write(expand)
    SinglebandGeoTiff(remainTiff, geoSingleTiff.extent, geoSingleTiff.crs).write(remain)
  }
}
