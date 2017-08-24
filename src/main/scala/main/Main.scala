package main

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import org.nd4s.Implicits._
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.factory.Nd4jBackend
import geotrellis.spark._
import geotrellis.spark.io._

object Main {
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
  def getRow (mulBandImg:String): Int = {
    val geoTiffMulBandImg:MultibandGeoTiff = GeoTiffReader.readMultiband(mulBandImg)
    var row: Int = geoTiffMulBandImg.tile.rows
    row
  }
  def getCol (mulBandImg:String): Int = {
    val geoTiffMulBandImg:MultibandGeoTiff = GeoTiffReader.readMultiband(mulBandImg)
    var col: Int = geoTiffMulBandImg.tile.cols
    col
  }
  def NDIndex (B1:Double, B2: Double): Float = {
    ((B1-B2)/(B1+B2)).toFloat
  }
  def Max (B1:Double, B2: Double, B3:Double, B4: Double):Double = {
    var max = B1
    if (max < B2) max = B2
    if (max < B3) max = B3
    if (max < B4) max = B4
    max
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

  def main(args: Array[String]): Unit = {

    val configSp: SparkConf = new SparkConf().setAppName("CoDe")
    // sparkContext is metadata about spark cluster used to creating RDD
    val sparkContext: SparkContext = new SparkContext(configSp)

    val pathSingleBand:String = "F:\\py_code_data\\TOA_VNR20150117_XS_coastal.tif"
    val pathMultipleBand:String = "F:\\py_code_data\\TOA_VNR20150117_PXS_Clip_coastal.tif"
    val geoTiffMul: MultibandGeoTiff = GeoTiffReader.readMultiband(pathMultipleBand)
    val dataBand:Array[Array[Double]] = Open4BandTif(pathMultipleBand)
    val Ysize = getRow(pathMultipleBand)
    val Xsize = getCol(pathMultipleBand)

    println("image size:" + "[" + Xsize + "," + Ysize + "]")
    //create thin cloud mask band
    var maskData = Nd4j.zeros(Ysize*Xsize)

    var DVGreenRed:Double = 0.0
    var DVMaxMin4:Double = 0.0
    var DVMaxMin:Double = 0.0
    var DVMaxMinRGB = 0.0

    var x,y = 0
    val imgSize = (Ysize*Xsize)
    for (index <- 0 until imgSize) {
      if (dataBand(2)(index) > 0 && dataBand(0)(index) > 0){
        DVGreenRed = NDIndex (dataBand(1)(index), dataBand(2)(index))
        DVMaxMin4 = Max(dataBand(0)(index), dataBand(1)(index), dataBand(2)(index), dataBand(3)(index))/Min(dataBand(0)(index), dataBand(1)(index), dataBand(2)(index), dataBand(3)(index))
        DVMaxMin = Max(0.0, dataBand(1)(index), dataBand(2)(index), dataBand(3)(index))/Min(1.0, dataBand(1)(index), dataBand(2)(index), dataBand(3)(index))
        DVMaxMinRGB = Max(0.0, dataBand(0)(index), dataBand(1)(index), dataBand(2)(index))/Min(1.0, dataBand(0)(index), dataBand(1)(index), dataBand(2)(index))
      }
      //no data
      if ( dataBand(0)(index) == 0 ) maskData(index) = -9999
      else if ( (dataBand(0)(index) > dataBand(1)(index))
        && (dataBand(1)(index) > dataBand(2)(index))
        && (dataBand(1)(index) > dataBand(3)(index))
        && (-0.038 < NDIndex(dataBand(3)(index), dataBand(2)(index)) && NDIndex(dataBand(3)(index), dataBand(2)(index)) < 0.0683)
        && (-0.0245 < NDIndex(dataBand(1)(index), dataBand(3)(index)) && NDIndex(dataBand(1)(index), dataBand(3)(index)) < 0.1054) ) maskData(index) = 1
      else if ( (DVMaxMin4 < 1.15 || (1.15 <= DVMaxMin4 && DVMaxMin < 1.09 && (-0.025 < DVGreenRed && DVGreenRed < 0.04) && DVMaxMinRGB < 1.28))
        && !(dataBand(3)(index) > dataBand(2)(index) && dataBand(2)(index) > dataBand(1)(index) && dataBand(1)(index) > dataBand (0)(index))
        && !(dataBand(0)(index) > dataBand(1)(index) && dataBand(1)(index) > dataBand(2)(index) && dataBand(1)(index) > dataBand(3)(index)) ) maskData(index) = 2
      else if ( (dataBand(3)(index) > dataBand(2)(index) && dataBand(2)(index) > dataBand(1)(index) && dataBand(1)(index) > dataBand (0)(index))
        && (dataBand(0)(index) > 0.40)
        && (NDIndex(dataBand(3)(index), dataBand(2)(index)) < 0.057)
        && (NDIndex(dataBand(3)(index), dataBand(0)(index)) < 0.136)
        && (NDIndex(dataBand(1)(index), dataBand(0)(index)) < 0.04)) maskData(index) = 4
      else maskData(index) = 0
    }
    println("2D maskdata array converting")

    var maskData2D = Array.ofDim[Double](Ysize, Xsize)
    for ( y <- 0 to  Ysize-1) {
      for ( x <- 0 to  Xsize-1) {
        maskData2D(y)(x) = maskData(x + Xsize*y)
      }
    }

    println("3D databand array converting")

    var dataBand3D = Array.ofDim[Double](4,Ysize, Xsize)
    for (index <- 0 to 3) {
      for (y <- 0 to Ysize - 1) {
        for (x <- 0 to Xsize - 1) {
          dataBand3D(index)(y)(x) = dataBand(index)(x + Xsize * y)
        }
      }
    }
    println("thick cloud processing")
    var band0 = 0.0
    var band1 = 0.0
    var band2 = 0.0
    var band3 = 0.0
    var countCloud = 0
    var k = 0
    var cloudWindow = Array.ofDim[Double](25)


    for (y <- 0 to Ysize - 1) {
      for (x <- 0 to Xsize - 1) {
        band0 = dataBand3D(0)(y)(x)
        band1 = dataBand3D(1)(y)(x)
        band2 = dataBand3D(2)(y)(x)
        band3 = dataBand3D(3)(y)(x)
        if (maskData2D(y)(x) == 0
          && (band3 > band2 && band2 > band1 && band1 > band0)
          && Brightness(band0, band1, band2, band3) > 0.25
          && NDIndex(band3, band2) < 0.06) {
          k = 0
          countCloud = 0
          for (j <- -2 to 2) {
            for (i <- -2 to 2) {
              if ( (j + y) >= 0 && (j + y) < Ysize && (i + x >= 0) && (i + x) < Xsize)
                cloudWindow(k) = maskData2D(y+j)(x+i)
              k = k + 1
            }
          }
          cloudWindow.foreach( a => {
            if (a > 0)
              countCloud = countCloud + 1
          })
          if (countCloud >= 9) maskData2D(y)(x) = 3
          else maskData2D(y)(x) = -9999
        }
      }
    }

    println("start smoothing processing")
    var maskDataFinal = Nd4j.zeros(Ysize,Xsize)
    for (y <- 0 to Ysize - 1) {
      for (x <- 0 to Xsize - 1) {
        band0 = dataBand3D(0)(y)(x)
        band1 = dataBand3D(1)(y)(x)
        band2 = dataBand3D(2)(y)(x)
        band3 = dataBand3D(3)(y)(x)
        k = 0
        countCloud = 0
        for (j <- -2 to 2) {
          for (i <- -2 to 2) {
            if ( (j + y) >= 0 && (j + y) < Ysize && (i + x >= 0) && (i + x) < Xsize)
              cloudWindow(k) = maskData2D(y+j)(x+i)
            k = k + 1
          }
        }
        cloudWindow.foreach( a => {
          if (a > 0)
            countCloud = countCloud + 1
        })
        if (countCloud >= 10) maskDataFinal(y,x) = 1
        else maskDataFinal(y,x) = 0
      }
    }
  }
}
