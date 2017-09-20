package main

import sys.process._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import java.awt.image.BufferedImage

import geotrellis.raster.{DoubleArrayTile, FloatCellType}
import geotrellis.raster.render._
import org.apache.spark.SparkContext


object Classification {
  def classificationRemain (changeImgPath: String, outname:String, trainPath: String, testPath: String, rb0:String , rb1: String, rb2: String, rb3:String): Unit = {
    println("-----------start classification remain----------")
    val command = "python F:\\CoDe\\src\\main\\scala\\main\\classification.py " + rb0 + " " + rb1 + " " + rb2 + " " + rb3 + " " + changeImgPath + " " + outname + " " + trainPath + " " + testPath
    command.!
    println(command)
  }
  def classificationExpand (remainImgPath: String, outname:String, trainPath: String, testPath: String, eb0:String , eb1: String, eb2: String, eb3:String): Unit = {
    println("-----------start classification expand----------")
    val command = "python F:\\CoDe\\src\\main\\scala\\main\\classification.py " + eb0 + " " + eb1 + " " + eb2 + " " + eb3 + " " + remainImgPath + " " + outname + " " + trainPath + " " + testPath
    command.!
    println(command)
  }
  def  aggregateResult (sc: SparkContext, expand: String, remain: String, result: String) : Unit = {
    val expandData: Array[Double] = Utilities.Open1BandTif(expand)
    val remainData: Array[Double] = Utilities.Open1BandTif(remain)
    val expandSingTif: SinglebandGeoTiff = GeoTiffReader.readSingleband(expand)
    val Ysize = Utilities.getRowSingleBand(expand)
    val Xsize = Utilities.getColSingleBand(expand)

    val resultData = sc.parallelize(expandData).zip(sc.parallelize(remainData)).map {
      case (a,b) => {
        if (a != 3 || b!= 3) {
          if (a != 3) {
            if (a == 0) a+5 else a
          }
          else b
        }
        else 3
      }
    }
    resultData.collect()
    val resultTiff = DoubleArrayTile(resultData.collect(), Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(resultTiff, expandSingTif.extent, expandSingTif.crs).write(result)
  }
  def createPNG (tifImg: String, colorExpand: String): Unit = {
    val geoTifSing: SinglebandGeoTiff = GeoTiffReader.readSingleband(tifImg)
    println("-----redering png----------")
    val colorRamp =
      ColorRamp(
        RGB(0,255,0),
        RGB(63, 255 ,51),
        RGB(102,255,102),
        RGB(178, 255,102),
        RGB(255,255,0),
        RGB(255,255,51),
        RGB(255,153, 51),
        RGB(255,128,0),
        RGB(255,51,51),
        RGB(255,0,0)
      )
    geoTifSing.tile.renderPng(colorRamp).write(colorExpand)
  }
}
