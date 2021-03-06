package main

import java.awt.image.DataBuffer

import geotrellis.raster.{DoubleArrayTile, FloatCellType}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import org.apache.spark.SparkContext
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4s.Implicits._

import scala.collection.mutable.ArrayBuffer

object RadiometricNormalization {
  def radiometricNormalization (sc: SparkContext, refImgPath: String, dstDataPath: String):Array[Array[Double]] = {
    val geoTiffMul: MultibandGeoTiff = GeoTiffReader.readMultiband(refImgPath)
    val srcImgDataBand:Array[Array[Double]] = Utilities.Open4BandTif(refImgPath)
    var dstDataBand: Array[Array[Double]] = Utilities.Open4BandTif(dstDataPath)
    val Ysize = Utilities.getRowMultiBand(refImgPath)
    val Xsize = Utilities.getColMultiBand(refImgPath)
    val imgSize = Ysize*Xsize

    var srcDataB0:ArrayBuffer[Double] = new ArrayBuffer[Double] ()
    var srcDataB1:ArrayBuffer[Double] = new ArrayBuffer[Double] ()
    var srcDataB2:ArrayBuffer[Double] = new ArrayBuffer[Double] ()
    var srcDataB3:ArrayBuffer[Double] = new ArrayBuffer[Double] ()

    var dstDataB0:ArrayBuffer[Double] = new ArrayBuffer[Double] ()
    var dstDataB1:ArrayBuffer[Double] = new ArrayBuffer[Double] ()
    var dstDataB2:ArrayBuffer[Double] = new ArrayBuffer[Double] ()
    var dstDataB3:ArrayBuffer[Double] = new ArrayBuffer[Double] ()

    // append data
    for (index <- 0 until imgSize) {
      if (dstDataBand(0)(index) > 0.0){
        srcDataB0.append(srcImgDataBand(0)(index))
        srcDataB1.append(srcImgDataBand(1)(index))
        srcDataB2.append(srcImgDataBand(2)(index))
        srcDataB3.append(srcImgDataBand(3)(index))


        dstDataB0.append(dstDataBand(0)(index))
        dstDataB1.append(dstDataBand(1)(index))
        dstDataB2.append(dstDataBand(2)(index))
        dstDataB3.append(dstDataBand(3)(index))
      }
    }
    // mean
    val srcMeanB0 = Utilities.mean(srcDataB0)
    val srcMeanB1 = Utilities.mean(srcDataB1)
    val srcMeanB2 = Utilities.mean(srcDataB2)
    val srcMeanB3 = Utilities.mean(srcDataB3)

    val dstMeanB0 = Utilities.mean(dstDataB0)
    val dstMeanB1 = Utilities.mean(dstDataB1)
    val dstMeanB2 = Utilities.mean(dstDataB2)
    val dstMeanB3 = Utilities.mean(dstDataB3)

    //std
    val srcStdB0 = Utilities.std(srcDataB0)
    val srcStdB1 = Utilities.std(srcDataB1)
    val srcStdB2 = Utilities.std(srcDataB2)
    val srcStdB3 = Utilities.std(srcDataB3)

    val dstStdB0 = Utilities.std(dstDataB0)
    val dstStdB1 = Utilities.std(dstDataB1)
    val dstStdB2 = Utilities.std(dstDataB2)
    val dstStdB3 = Utilities.std(dstDataB3)

    println("mean and std of src")
    println(srcMeanB0 + ", " + srcStdB0)
    println(srcMeanB1 + ", " + srcStdB1)
    println(srcMeanB2 + ", " + srcStdB2)
    println(srcMeanB3 + ", " + srcStdB3)

    println("mean and std of dst")
    println(dstMeanB0 + ", " + dstStdB0)
    println(dstMeanB1 + ", " + dstStdB1)
    println(dstMeanB2 + ", " + dstStdB2)
    println(dstMeanB3 + ", " + dstStdB3)

    // parallel
//    var dstDataB0Rdd = sc.parallelize(dstDataBand(0))
//    var dstDataB1Rdd = sc.parallelize(dstDataBand(1))
//    var dstDataB2Rdd = sc.parallelize(dstDataBand(2))
//    var dstDataB3Rdd = sc.parallelize(dstDataBand(3))

    var arrResult = Array.ofDim[Double](4, imgSize)

    for (index <- 0 until imgSize) {
      if (dstDataBand(0)(index) > 0.0) {
        arrResult(0)(index) = ((srcStdB0/dstStdB0)*(dstDataBand(0)(index) - dstMeanB0) + srcMeanB0)
        arrResult(1)(index) = ((srcStdB1/dstStdB1)*(dstDataBand(1)(index) - dstMeanB1) + srcMeanB1)
        arrResult(2)(index) = ((srcStdB2/dstStdB2)*(dstDataBand(2)(index) - dstMeanB2) + srcMeanB2)
        arrResult(3)(index) = ((srcStdB3/dstStdB3)*(dstDataBand(3)(index) - dstMeanB3) + srcMeanB3)
      }
      else {
        arrResult(0)(index) = Double.NaN
        arrResult(1)(index) = Double.NaN
        arrResult(2)(index) = Double.NaN
        arrResult(3)(index) = Double.NaN
      }
    }
//    dstDataB0Rdd.map( e => if (e>0) (srcStdB0/dstStdB0)*(e - dstMeanB0) + srcMeanB0)
//    dstDataB1Rdd.map( e => if (e>0) (srcStdB1/dstStdB1)*(e - dstMeanB1) + srcMeanB1)
//    dstDataB2Rdd.map( e => if (e>0) (srcStdB2/dstStdB2)*(e - dstMeanB2) + srcMeanB2)
//    dstDataB3Rdd.map( e => if (e>0) (srcStdB3/dstStdB3)*(e - dstMeanB3) + srcMeanB3)
//    // collect data
//    dstDataBand(0) = dstDataB0Rdd.collect()
//    dstDataBand(1) = dstDataB1Rdd.collect()
//    dstDataBand(2) = dstDataB2Rdd.collect()
//    dstDataBand(3) = dstDataB3Rdd.collect()
    //return
    arrResult
  }
}
