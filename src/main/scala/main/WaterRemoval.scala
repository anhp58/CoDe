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
    val dataBandRdd_0 = sc.parallelize(dataBand(0))
    val dataBandRdd_3 = sc.parallelize(dataBand(3))
    //distributed computing
    val zipData = dataBandRdd_0.zip(dataBandRdd_3).map { case (a, b) => {if (Utilities.waterIndex(a, b) <= 0.1) 1.0 else 0.0} }

    //collect local data
    tiffMaskFinalData = zipData.collect()
    // write tiff
    val Tiff = DoubleArrayTile(tiffMaskFinalData, Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(Tiff, geoTiffMul.extent, geoTiffMul.crs).write(waterMaskFileName)
    println("done removing water")
  }
}
