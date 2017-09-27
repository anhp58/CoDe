package main
import geotrellis.raster.{DoubleArrayTile, FloatArrayTile, FloatCellType}
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}


object DetectChange {
  def kapur (imgArray: Array[Double]): Double = {
    val len:Int = imgArray.length
    var threshold:Double = - 1
    var total:Double = 0
    var ih:Double = 0
    var it:Double = 0
    for (ih <- 0 until len) {
      total = total + imgArray(ih)
    }
    var norHisto:Array[Float] = Array.ofDim[Float](len)
    for (ih <- 0 until len) {
      norHisto(ih) = (imgArray(ih)/total).toFloat
    }
    var P1:Array[Float] = Array.ofDim[Float](len)
    var P2:Array[Float] = Array.ofDim[Float](len)
    P1(0) = norHisto(0)
    P2(0) = (1.0 - P1(0)).toFloat
    for (ih <- 1 until len) {
      P1(ih) = P1(ih -1) + norHisto(ih)
      P2(ih) = 1 - P1(ih)
    }
    var maxEnt = -10.0
    threshold = 0.0
    for (it <- 0 until len ) {
      // entropy of the backgroud
      var entBack = 0.0
      for (ih <- 0 until it+1) {
        if (imgArray(ih) != 0) {
          entBack = entBack - (norHisto(ih)/P1(it))*Math.log(norHisto(ih)/P1(it))
        }
      }
      // entropy of the object pixels
      var entObj = 0.0
      for (ih <- it+1 until len ) {
        if (imgArray(ih) != 0) {
          entObj = entObj - (norHisto(ih)/P2(it))*Math.log(norHisto(ih)/P2(it))
        }
      }
      // total entropy
      var totalEnt = entBack + entObj
      if (maxEnt < totalEnt) {
        maxEnt = totalEnt
        threshold = it
      }
    }
    threshold
  }
  def huang (data: Array[Double]): Double = {
    var threshold: Double = -1
    var fisrtBin = 0
    var arrFirst:ArrayBuffer[Int] = new ArrayBuffer[Int] ()
    for  (ih <- 0 until 254) {
      if (data(ih) != 0) {
        arrFirst.append(ih)
      }
    }
    fisrtBin = arrFirst(0)
    println("first bin" + fisrtBin)
    var lastBin = 254
    var arrLast:ArrayBuffer[Int] = new ArrayBuffer[Int] ()
    for (ih <- (0 until 255).reverse) {
      if (data(ih) != 0) {
        arrLast.append(ih)
      }
    }
    lastBin = arrLast(0)
    println("last bin" + lastBin)
    var term = 1.0/(lastBin - fisrtBin)
    var mu0: Array[Double] = Array.ofDim[Double](254)
    var numPix = 0.0
    var sumPix = 0.0
    for ( ih <- fisrtBin until 254) {
      sumPix = sumPix + (ih * data(ih))
      numPix = numPix + data(ih)
      mu0(ih) = sumPix/numPix
    }
    var mu1: Array[Double] = Array.ofDim[Double](254)
    sumPix = 0.0
    numPix = 0.0
    for ( ih <- lastBin until 1 by -1){
      sumPix = sumPix + (ih * data(ih))
      numPix = numPix + data(ih)
      mu1(ih-1) = sumPix/numPix
    }
    var minEnt:Float = 9999999
    for (it <- 0 until 254) {
      var ent:Float = 0
      for (ih <- 0 until it) {
        var muX = 1.0 / (1.0 + term * Math.abs((ih - mu0(it)).toFloat))
        if (!((muX < 1e-06) || (muX > 0.999999))) {
          ent = (ent + data(ih) * (-muX * Math.log(muX) - (1.0 - muX) * Math.log(1.0 - muX))).toFloat
        }
      }
      for (ih <- it + 1 until 254) {
        var muX = 1.0 / (1.0 + term * Math.abs((ih - mu1(it)).toFloat))
        if (!((muX < 1e-06) || (muX > 0.999999))) {
          ent = (ent + data(ih) * (-muX * Math.log(muX) - (1.0 - muX) * Math.log(1.0 - muX))).toFloat
        }
      }
      if (ent < minEnt) {
        minEnt = ent
        threshold = it
      }
    }
    threshold
  }
  def preprocessing (data: Array[Double]): Array[Double] = {
    var maxVal: Double = 0.0
    var minVal: Double = 10.0
    for (index <- 0 until data.length) {
      if (data(index) > 0.0) {
        if (data(index) > maxVal) maxVal = data(index)
        if (data(index) < minVal) minVal = data(index)
      }
    }
    var dataKapur: Array[Double] = Array.fill[Double](255)(0)
    var index: Int = 0
    for ( i <- 0 until data.length) {
      if (data(i) > 0.0) {
        index = Math.round((data(i)*200)).toInt
        if (index < 255) dataKapur(index) = dataKapur(index) + 1
      }
    }
    dataKapur
  }
  def detectChange (sc:SparkContext, ndImgPath: String, brightImgPath: String, changeImgPath : String): Unit = {
    val ndviSing: SinglebandGeoTiff = GeoTiffReader.readSingleband(ndImgPath)
    val ndviSingDataBand:Array[Double] = Utilities.Open1BandTif(ndImgPath)

    val Ysize = Utilities.getRowSingleBand(ndImgPath)
    val Xsize = Utilities.getColSingleBand(ndImgPath)

    val brightSing: SinglebandGeoTiff = GeoTiffReader.readSingleband(brightImgPath)
    val brightSingDataBand:Array[Double] = Utilities.Open1BandTif(brightImgPath)

    val dataHuang = preprocessing(ndviSingDataBand)
    val dataKapur = preprocessing(brightSingDataBand)

    var thresholdHuang: Double = 0
    var thresholdkapur: Double = 0

    if (!dataHuang.isEmpty) thresholdHuang = huang(dataHuang)*0.005

    println("ok")
    println("Huang" + thresholdHuang)

    if (!dataKapur.isEmpty) thresholdkapur = kapur(dataKapur)*0.005

    println("ok")
    println("Kapur" + thresholdkapur)
    thresholdHuang = 0.045
    thresholdkapur = 0.155

    val newdata: RDD[Double] = sc.parallelize(brightSingDataBand).zip(sc.parallelize(ndviSingDataBand)).map {
      case (a,b) => if ((b > thresholdHuang) || (a > thresholdkapur)) 1 else 0
    }
    val resultTiff = DoubleArrayTile(newdata.collect(), Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(resultTiff, ndviSing.extent, ndviSing.crs).write(changeImgPath)
  }
}
