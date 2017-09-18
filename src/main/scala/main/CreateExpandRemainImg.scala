package main

import geotrellis.raster.{ArrayMultibandTile, DoubleArrayTile, DoubleCellType, FloatCellType, IntConstantNoDataCellType}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import org.apache.spark.SparkContext

object CreateExpandRemainImg {
  def createExpandRemaining (expandMask: Array[Double], remainMask: Array[Double], targetPath: String, sc: SparkContext, expand: String, remain: String): Unit = {
    //read 4 band img
    val geoTiffMul: MultibandGeoTiff = GeoTiffReader.readMultiband(targetPath)
    val dataBand:Array[Array[Double]] = Utilities.Open4BandTif(targetPath)
    val Ysize = Utilities.getRowMultiBand(targetPath)
    val Xsize = Utilities.getColMultiBand(targetPath)
//    geoTiffMul.raster.band(1).



    // parallel
//    val expandMaskZip = sc.parallelize(expandMask)
//    val remainMaskZip = sc.parallelize(remainMask)
//    // parallel expand
//    val expandDataBand0Zip = sc.parallelize(dataBand(0))
//                              .zip(expandMaskZip).map { case (x,y) => { if (Utilities.comparison(y)) x else 0 }}
//    val expandDataBand1Zip = sc.parallelize(dataBand(1))
//                              .zip(expandMaskZip).map { case (x,y) => { if (Utilities.comparison(y)) x else 0 }}
//    val expandDataBand2Zip = sc.parallelize(dataBand(2))
//                              .zip(expandMaskZip).map { case (x,y) => { if (Utilities.comparison(y)) x else 0 }}
//    val expandDataBand3Zip = sc.parallelize(dataBand(3))
//                              .zip(expandMaskZip).map { case (x,y) => { if (Utilities.comparison(y)) x else 0 }}
//
//    // parallel remain
//    val remainDataBand0Zip = sc.parallelize(dataBand(0))
//                              .zip(remainMaskZip).map { case (x,y) => { if (Utilities.comparison(y)) x else 0 }}
//    val remainDataBand1Zip = sc.parallelize(dataBand(1))
//                              .zip(remainMaskZip).map { case (x,y) => { if (Utilities.comparison(y)) x else 0 }}
//    val remainDataBand2Zip = sc.parallelize(dataBand(2))
//                              .zip(remainMaskZip).map { case (x,y) => { if (Utilities.comparison(y)) x else 0 }}
//    val remainDataBand3Zip = sc.parallelize(dataBand(3))
//                              .zip(remainMaskZip).map { case (x,y) => { if (Utilities.comparison(y)) x else 0 }}
//
//    val expandDataBand = Array.ofDim[Double](4, Ysize*Xsize)
//    val remainDataBand = Array.ofDim[Double](4, Ysize*Xsize)
//
//    expandDataBand(0) = expandDataBand0Zip.collect()
//    expandDataBand(1) = expandDataBand1Zip.collect()
//    expandDataBand(2) = expandDataBand2Zip.collect()
//    expandDataBand(3) = expandDataBand3Zip.collect()
//
//    remainDataBand(0) = remainDataBand0Zip.collect()
//    remainDataBand(1) = remainDataBand1Zip.collect()
//    remainDataBand(2) = remainDataBand2Zip.collect()
//    remainDataBand(3) = remainDataBand3Zip.collect()
//
  }
}
