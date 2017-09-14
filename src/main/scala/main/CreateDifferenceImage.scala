package main

import geotrellis.raster.{DoubleArrayTile, FloatArrayTile, FloatCellType}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import org.apache.spark.SparkContext

object CreateDifferenceImage {
  def createDifferenceImage (sc: SparkContext, srcNdvi: String, srcNd31: String, srcBright: String, dstNdvi: String, dstNd31: String, dstBright: String, differencePath: String, brightPath: String): Unit = {
    val srcNdviSing: SinglebandGeoTiff = GeoTiffReader.readSingleband(srcNdvi)
    val srcDataBandNdvi:Array[Double] = Utilities.Open1BandTif(srcNdvi)

    val srcNd31Sing: SinglebandGeoTiff = GeoTiffReader.readSingleband(srcNd31)
    val srcDataBandNd31:Array[Double] = Utilities.Open1BandTif(srcNd31)

    val srcBrightSing: SinglebandGeoTiff = GeoTiffReader.readSingleband(srcBright)
    val srcDataBandBright:Array[Double] = Utilities.Open1BandTif(srcBright)

    val dstNdviSing: SinglebandGeoTiff = GeoTiffReader.readSingleband(dstNdvi)
    val dstDataBandNdvi:Array[Double] = Utilities.Open1BandTif(dstNdvi)

    val dstNd31Sing: SinglebandGeoTiff = GeoTiffReader.readSingleband(dstNd31)
    val dstDataBandNd31:Array[Double] = Utilities.Open1BandTif(dstNd31)

    val dstBrightSing: SinglebandGeoTiff = GeoTiffReader.readSingleband(dstBright)
    val dstDataBandBright:Array[Double] = Utilities.Open1BandTif(dstBright)

    val Ysize = Utilities.getRowSingleBand(srcNdvi)
    val Xsize = Utilities.getColSingleBand(srcNdvi)
    val imgSize = Ysize*Xsize

    // parallel
//    val differenceDataRdd = sc.parallelize(srcDataBandNdvi)
//                              .zip(sc.parallelize(srcDataBandNd31))
//                              .zip(sc.parallelize(dstDataBandNdvi))
//                              .zip(sc.parallelize(dstDataBandNd31)).map {
//      case ((((a,b),c),d)) => { if (Math.abs(d) > 0.0) Math.sqrt((b-d)*(b-d) + (a-c)*(a-c)) else Double.NaN}
//    }
//    val brightDataRdd = sc.parallelize(srcDataBandBright)
//                          .zip(sc.parallelize(dstDataBandBright))
//                          .zip(sc.parallelize(dstDataBandNd31)).map {
//      case (((a,b),c)) => { if (Math.abs(c) > 0.0) Math.abs(a-b) else Double.NaN}
//    }
    // create tiff img
//    val differenceTiff = DoubleArrayTile(differenceDataRdd.collect(), Xsize, Ysize).convert(FloatCellType)
//    SinglebandGeoTiff(differenceTiff, dstNdviSing.extent, dstNdviSing.crs).write(differencePath)
//    val brightTiff = DoubleArrayTile(brightDataRdd.collect(), Xsize, Ysize).convert(FloatCellType)
//    SinglebandGeoTiff(brightTiff, dstNdviSing.extent, dstNdviSing.crs).write(brightPath)
    var differenceData:Array[Double] = Array.ofDim[Double](imgSize)
    var brightData:Array[Double] = Array.ofDim[Double](imgSize)
    for ( index <- 0 until imgSize) {
      if (Math.abs(dstDataBandNd31(index)) > 0.0) {
        differenceData(index) = Math.sqrt(Math.pow((srcDataBandNd31(index) - dstDataBandNd31(index)), 2) + Math.pow((srcDataBandNdvi(index) - dstDataBandNdvi(index)), 2))
        brightData(index) = Math.abs(srcDataBandBright(index) - dstDataBandBright(index))
      }
      else {
        differenceData(index) = Double.NaN
        brightData(index) = Double.NaN
      }
    }
    val differenceTiff = DoubleArrayTile(differenceData, Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(differenceTiff, dstNdviSing.extent, dstNdviSing.crs).write(differencePath)
    val brightTiff = DoubleArrayTile(brightData, Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(brightTiff, dstNdviSing.extent, dstNdviSing.crs).write(brightPath)
  }
}
