package main

import java.io.File

import sys.process._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.{DoubleArrayTile, DoubleCellType, FloatCellType}
import org.apache.spark.SparkContext
import main.Utils.{loadOrExit, show}
import javax.swing.JFrame

import geotrellis.spark
import org.bytedeco.javacpp.opencv_imgcodecs.{IMREAD_COLOR, imread, _}
import org.bytedeco.javacpp.opencv_imgproc._
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint




object Classification {
  def classificationRemain (sc:SparkContext, changeImgPath: String, outname:String, trainPath: String, testPath: String, rb0:String , rb1: String, rb2: String, rb3:String): Unit = {
//    println("-----------start classification remain----------")
//    val command = "python F:\\CoDe\\src\\main\\scala\\main\\classification.py " + rb0 + " " + rb1 + " " + rb2 + " " + rb3 + " " + changeImgPath + " " + outname + " " + trainPath + " " + testPath
//    command.!
//    println(command)
    val spark = SparkSession
      .builder
      .appName("LinearSVCExample")
      .getOrCreate()
    val training = spark.read.format("libsvm").load(trainPath)
    val testing = spark.read.format("libsvm").load(testPath)
    val lsvc = new LinearSVC()
    val lsvcModel = lsvc.fit(testing)
//    testing.foreach(println(_))
//    lsvcModel.setThreshold(0)
//    lsvcModel.transform(testing).show(400)
    //classification data

    val rb0Data: Array[Double] = Utilities.Open1BandTif(rb0)
    val rb1Data: Array[Double] = Utilities.Open1BandTif(rb1)
    val rb2Data: Array[Double] = Utilities.Open1BandTif(rb2)
    val rb3Data: Array[Double] = Utilities.Open1BandTif(rb3)
    val changeData: Array[Double] = Utilities.Open1BandTif(changeImgPath)


    val Xsize = Utilities.getColMultiBand(rb0)
    val Ysize = Utilities.getRowMultiBand(rb0)

    var classTestData: Seq[LabeledPoint] = Seq[LabeledPoint]()
//    while (i < Ysize*Xsize ) {
//      if (rb0Data(i) != Double.NaN)
//        classTestData :+= LabeledPoint(0.0, Vectors.sparse(4, Array(0,1,2,3), Array(rb0Data(i), rb1Data(i), rb2Data(i), rb3Data(i))))
//      i += 1
//    }
    val imgSize = Ysize*Xsize
//    println("caculating")
//    println(imgSize - Ysize*500)
    var classRemainArr: Array[Double] = Array.ofDim[Double](Xsize*Ysize)
    for (i <- 0 until imgSize) {
      if (changeData(i) == 1) {
        if (!rb0Data(i).isNaN) {
          classTestData = Seq(LabeledPoint(0.0, Vectors.sparse(4, Array(0, 1, 2, 3), Array(rb0Data(i), rb1Data(i), rb2Data(i), rb3Data(i)))))
          var remainDataDF: DataFrame = spark.createDataFrame(classTestData).toDF()
          var prediction = lsvcModel.transform(remainDataDF).select("prediction").rdd.map( r => r(0)).collect()
          classRemainArr(i) = prediction(0).toString.toDouble
        }
      } else classRemainArr(i) = 3
    }
    val geoTiffSing: SinglebandGeoTiff = GeoTiffReader.readSingleband(rb0)
    val Tiff = DoubleArrayTile(classRemainArr, Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(Tiff, geoTiffSing.extent, geoTiffSing.crs).write(outname)

  }
  def classificationExpand (changeImgPath: String, outname:String, trainPath: String, testPath: String, eb0:String , eb1: String, eb2: String, eb3:String): Unit = {
    println("-----------start classification expand----------")
    val command = "python F:\\CoDe\\src\\main\\scala\\main\\classify_extend.py " + eb0 + " " + eb1 + " " + eb2 + " " + eb3 + " " + changeImgPath + " " + outname + " " + trainPath + " " + testPath
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
//    val geoTifSing: SinglebandGeoTiff = GeoTiffReader.readSingleband(tifImg)
//    SinglebandGeoTiff(tifImg).tile.convert(DoubleCellType).renderPng().write(colorExpand)
    println("-----redering png----------")
    val img = imread(tifImg + ".TIF", CV_LOAD_IMAGE_ANYCOLOR)
//    val img = loadOrExit(new File(tifImg + ".TIF"), CV_LOAD_IMAGE_ANYCOLOR)
    imwrite(colorExpand + ".png", img)
//    println(new File(colorExpand))
//    val img: BufferedImage  = ImageIO.read(new FileInputStream(tifImg))
//    ImageIO.write(img, "png", new File(colorExpand))
//    var img2 = ImageIO.read(new File("C:\\data\\ClassificationResult\\ColorResult\\Expand_PXS_Classified.png"))
//    var count = 0
//    for (x <-0 until img.getWidth()) {
//      for (y <- 0 until  img.getHeight()) {
//        if (img.getRGB(x,y) != 0) img.setRGB(x,y,255)
//      }
//    }
//    println(count)
  }
}
