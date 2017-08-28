package main

import geotrellis.raster.{DoubleArrayTile, FloatCellType}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import org.nd4j.linalg.factory.Nd4j
import org.nd4s.Implicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WaterRemoval {
  def waterRemoval (pathMultipleBand: String, sc: SparkContext): Unit = {
    val geoTiffMul: MultibandGeoTiff = GeoTiffReader.readMultiband(pathMultipleBand)
    val dataBand:Array[Array[Double]] = Utilities.Open4BandTif(pathMultipleBand)
    val Ysize = Utilities.getRowMultiBand(pathMultipleBand)
    val Xsize = Utilities.getColMultiBand(pathMultipleBand)
    var imgSize = (Ysize*Xsize)
    val waterMaskFileName = Utilities.setMaskWaterName("TOA_VNR20150117_PXS_Clip_coastal")
    var waterMask = Nd4j.zeros(Ysize*Xsize)
    var waterIndex = 0.0

    var tiffMaskFinalData = Array.ofDim[Double](Ysize*Xsize)

    for (index <- 0 until imgSize){
      tiffMaskFinalData(index) = waterMask(index)
    }

    //load data to RDD
//    var waterMaskRdd = sc.parallelize(tiffMaskFinalData)
//    val dataBandRdd_0 = sc.parallelize(dataBand(0))
//    val dataBandRdd_3 = sc.parallelize(dataBand(3))

    for (index <- 0 until imgSize) {
      if (dataBand(0)(index) <= 0.0) tiffMaskFinalData(index) = -9999
      else {
        waterIndex = (dataBand(0)(index) - dataBand(3)(index))/(dataBand(0)(index) + dataBand(3)(index))
        if (waterIndex <= 0.1) tiffMaskFinalData(index) = 1
        else tiffMaskFinalData(index) = 0
      }
    }

    val Tiff = DoubleArrayTile(tiffMaskFinalData, Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(Tiff, geoTiffMul.extent, geoTiffMul.crs).write(waterMaskFileName)
    println("done removing water")
  }
}
