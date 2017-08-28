package main

import geotrellis.raster.{DoubleArrayTile, FloatCellType}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import org.nd4j.linalg.factory.Nd4j
import org.nd4s.Implicits._

object WaterRemoval {
  def waterRemoval (pathMultipleBand: String): Unit = {
    val geoTiffMul: MultibandGeoTiff = GeoTiffReader.readMultiband(pathMultipleBand)
    val dataBand:Array[Array[Double]] = Utilities.Open4BandTif(pathMultipleBand)
    val Ysize = Utilities.getRow(pathMultipleBand)
    val Xsize = Utilities.getCol(pathMultipleBand)
    var imgSize = (Ysize*Xsize)
    val waterMaskFileName = Utilities.setMaskWaterName("TOA_VNR20150117_PXS_Clip_coastal")
    var waterMask = Nd4j.zeros(Ysize*Xsize)
    var waterIndex = 0.0
    for (index <- 0 until imgSize) {
      if (dataBand(0)(index) <= 0.0) waterMask(index) = -9999
      else {
        waterIndex = (dataBand(0)(index) - dataBand(3)(index))/(dataBand(0)(index) + dataBand(3)(index))
        if (waterIndex <= 0.1) waterMask(index) = 1
        else waterMask(index) = 0
      }
    }
    var tiffMaskFinalData = Array.ofDim[Double](Ysize*Xsize)
    for (index <- 0 until imgSize) {
      tiffMaskFinalData(index) = waterMask(index)
    }
    val Tiff = DoubleArrayTile(tiffMaskFinalData, Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(Tiff, geoTiffMul.extent, geoTiffMul.crs).write(waterMaskFileName)
    println("done removing water")
  }
}
