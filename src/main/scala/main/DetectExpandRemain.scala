package main

import geotrellis.raster.{ArrayMultibandTile, DoubleArrayTile, FloatCellType}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import org.nd4j.linalg.factory.Nd4j
import org.nd4s.Implicits._
import geotrellis.raster._

object DetectExpandRemain {
  def detectExpandRemain (target25: String, refCloud : String, refWater: String, targetCloud: String, targetWater: String, rb0Path: String, rb1Path: String, rb2Path: String, rb3Path: String, eb0Path: String, eb1Path: String, eb2Path: String, eb3Path: String): Array[Array[Array[Double]]] = {
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
          expandDataBand(bIndex)(index) = Float.NaN
          counte = counte + 1
        }
        if (maskDataRemain(index) != 1.0) {
          remainDataBand(bIndex)(index) = Float.NaN
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

    println("------------------img extent----------------------")
    println(geoTiffMul.extent.height)
    println(geoTiffMul.extent.width)
    println(geoTiffMul.crs)
    val tiffRemainB0 = DoubleArrayTile(expandDataBand(0), Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(tiffRemainB0, geoTiffMul.extent, geoTiffMul.crs).write(rb0Path)
    val tiffRemainB1 = DoubleArrayTile(expandDataBand(1), Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(tiffRemainB0, geoTiffMul.extent, geoTiffMul.crs).write(rb1Path)
    val tiffRemainB2 = DoubleArrayTile(expandDataBand(2), Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(tiffRemainB0, geoTiffMul.extent, geoTiffMul.crs).write(rb2Path)
    val tiffRemainB3 = DoubleArrayTile(expandDataBand(3), Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(tiffRemainB0, geoTiffMul.extent, geoTiffMul.crs).write(rb3Path)

    val tiffExpandB0 = DoubleArrayTile(expandDataBand(0), Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(tiffExpandB0, geoTiffMul.extent, geoTiffMul.crs).write(eb0Path)
    val tiffExpandB1 = DoubleArrayTile(expandDataBand(1), Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(tiffExpandB1, geoTiffMul.extent, geoTiffMul.crs).write(eb1Path)
    val tiffExpandB2 = DoubleArrayTile(expandDataBand(2), Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(tiffExpandB2, geoTiffMul.extent, geoTiffMul.crs).write(eb2Path)
    val tiffExpandB3 = DoubleArrayTile(expandDataBand(3), Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(tiffExpandB3, geoTiffMul.extent, geoTiffMul.crs).write(eb3Path)
    ArrayResult
  }
}
