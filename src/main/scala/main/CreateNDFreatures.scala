package main

import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.{DoubleArrayTile, FloatArrayTile, FloatCellType}
import org.apache.spark.SparkContext

object CreateNDFreatures {
  def createNDFreatures (imgPath: String, sc: SparkContext, norDataBand: Array[Array[Double]], ndviPath: String, nd31Path: String, brightPath: String): Unit = {
    val geoTiffMul: MultibandGeoTiff = GeoTiffReader.readMultiband(imgPath)
    val Ysize = Utilities.getRowMultiBand(imgPath)
    val Xsize = Utilities.getColMultiBand(imgPath)

    val ndviDataRdd = sc.parallelize(norDataBand(3)).zip(sc.parallelize(norDataBand(2))).map {
      case (a,b) => Utilities.NDIndex(a,b)
    }
    val nd31DataRdd = sc.parallelize(norDataBand(2)).zip(sc.parallelize(norDataBand(0))).map {
      case (a,b) => Utilities.NDIndex(a,b)
    }
    val brightDataRdd = sc.parallelize(norDataBand(0))
      .zip(sc.parallelize(norDataBand(1)))
      .zip(sc.parallelize(norDataBand(2)))
      .zip(sc.parallelize(norDataBand(3))).map {
      case ((((a,b),c), d)) => Utilities.Brightness(a,b,c,d)
    }
    val ndviTiff = FloatArrayTile(ndviDataRdd.collect(), Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(ndviTiff, geoTiffMul.extent, geoTiffMul.crs).write(ndviPath)
    val nd31Tiff = FloatArrayTile(nd31DataRdd.collect(), Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(nd31Tiff, geoTiffMul.extent, geoTiffMul.crs).write(nd31Path)
    val brightTiff = FloatArrayTile(brightDataRdd.collect(), Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(brightTiff, geoTiffMul.extent, geoTiffMul.crs).write(brightPath)
  }
}
