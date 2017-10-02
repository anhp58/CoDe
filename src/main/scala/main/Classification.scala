package main

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.{File, FileInputStream}
import javax.imageio.ImageIO

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
import org.bytedeco.javacpp.opencv_core.Mat
import org.bytedeco.javacpp.opencv_ml.LogisticRegression

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
    val lsvc = new LinearSVC().setWeightCol("classWeightCol").setLabelCol("label").setFeaturesCol("features")
    val lsvcModel = lsvc.fit(Utilities.balanceDataset(training))
    //classification data

    val rb0Data: Array[Double] = Utilities.Open1BandTif(rb0)
    val rb1Data: Array[Double] = Utilities.Open1BandTif(rb1)
    val rb2Data: Array[Double] = Utilities.Open1BandTif(rb2)
    val rb3Data: Array[Double] = Utilities.Open1BandTif(rb3)
    val changeData: Array[Double] = Utilities.Open1BandTif(changeImgPath)


    val Xsize = Utilities.getColMultiBand(rb0)
    val Ysize = Utilities.getRowMultiBand(rb0)

    var classTestData: Seq[LabeledPoint] = Seq[LabeledPoint]()
    val imgSize = Ysize*Xsize
    var classRemainArr: Array[Double] = Array.ofDim[Double](Xsize*Ysize)
    var remainDataDF: DataFrame = spark
      .createDataFrame(Seq(LabeledPoint(0.0, Vectors.sparse(4, Array(0, 1, 2, 3), Array(rb0Data(0), rb1Data(0), rb2Data(0), rb3Data(0))))))
      .toDF()
    for (i <- 0 until imgSize) {
      if (changeData(i) == 1) {
        if (!rb0Data(i).isNaN) {
          remainDataDF = spark.createDataFrame(Seq(LabeledPoint(0.0, Vectors.sparse(4, Array(0, 1, 2, 3), Array(rb0Data(i), rb1Data(i), rb2Data(i), rb3Data(i))))))
            .toDF()
          classRemainArr(i) = lsvcModel.transform(Utilities.balanceDataset(remainDataDF)).select("prediction").collectAsList().get(0)(0).toString.toDouble
        }
      } else classRemainArr(i) = 3
    }
    val geoTiffSing: SinglebandGeoTiff = GeoTiffReader.readSingleband(rb0)
    val Tiff = DoubleArrayTile(classRemainArr, Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(Tiff, geoTiffSing.extent, geoTiffSing.crs).write(outname)

  }
  def classificationExpand (sc:SparkContext, changeImgPath: String, outname:String, trainPath: String, testPath: String, eb0:String , eb1: String, eb2: String, eb3:String): Unit = {
    val spark = SparkSession
      .builder
      .appName("LinearSVCExample")
      .getOrCreate()
    val training = spark.read.format("libsvm").load(trainPath)
    val testing = spark.read.format("libsvm").load(testPath)
    val lsvc = new LinearSVC().setWeightCol("classWeightCol").setLabelCol("label").setFeaturesCol("features")
    val lsvcModel = lsvc.fit(Utilities.balanceDataset(training))
    //classification data
    val eb0Data: Array[Double] = Utilities.Open1BandTif(eb0)
    val eb1Data: Array[Double] = Utilities.Open1BandTif(eb1)
    val eb2Data: Array[Double] = Utilities.Open1BandTif(eb2)
    val eb3Data: Array[Double] = Utilities.Open1BandTif(eb3)
    val changeData: Array[Double] = Utilities.Open1BandTif(changeImgPath)

    val Xsize = Utilities.getColMultiBand(eb0)
    val Ysize = Utilities.getRowMultiBand(eb0)

    var classTestData: Seq[LabeledPoint] = Seq[LabeledPoint]()
    val imgSize = Ysize*Xsize
    var classExpandArr: Array[Double] = Array.ofDim[Double](Xsize*Ysize)
    var expandDataDF: DataFrame = spark
      .createDataFrame(Seq(LabeledPoint(0.0, Vectors.sparse(4, Array(0, 1, 2, 3), Array(eb0Data(0), eb1Data(0), eb2Data(0), eb3Data(0))))))
      .toDF()
    for (i <- 0 until imgSize) {
      if (eb0Data(i) > 0) {
        if (!eb0Data(i).isNaN) {
          expandDataDF = spark
            .createDataFrame(Seq(LabeledPoint(0.0, Vectors.sparse(4, Array(0, 1, 2, 3), Array(eb0Data(i), eb1Data(i), eb2Data(i), eb3Data(i))))))
            .toDF()
          classExpandArr(i) = lsvcModel.transform(Utilities.balanceDataset(expandDataDF)).select("prediction").collectAsList().get(0)(0).toString.toDouble
        }
      } else classExpandArr(i) = 3
    }
//    classTestData = Seq(LabeledPoint(0.0, Vectors.sparse(4, Array(0, 1, 2, 3), Array(eb0Data(1), eb1Data(1), eb2Data(1), eb3Data(1)))))
//    var expandDataDF: DataFrame = spark.createDataFrame(classTestData).toDF()
////    val prediction = lsvcModel.transform(expandDataDF).select("prediction").rdd.map( r=> r(0)).collect()
//    val prediction = lsvcModel.transform(expandDataDF).select("prediction").collectAsList().get(0)(0)
//    println(prediction)
    val geoTiffSing: SinglebandGeoTiff = GeoTiffReader.readSingleband(eb0)
    val Tiff = DoubleArrayTile(classExpandArr, Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(Tiff, geoTiffSing.extent, geoTiffSing.crs).write(outname)
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
    val Ysize = Utilities.getRowMultiBand(tifImg)
    val Xsize = Utilities.getColMultiBand(tifImg)
    // define color
    val white = Color.white.getRGB // Color white
    val yellow = Color.yellow.getRGB
    val brown = new Color(165,42,42).getRGB
    val pink = new Color(255, 20,147).getRGB
    println("-----redering png----------")
    //read tiff file
    val img:Mat  = imread(tifImg,COLOR_GRAY2BGR)
    //write png file
    imwrite(colorExpand + ".png", img)
    //read png file
    val png: BufferedImage  = ImageIO.read(new FileInputStream(colorExpand + ".png"))
    val pngResult: BufferedImage = new BufferedImage(png.getWidth, png.getHeight, BufferedImage.TYPE_3BYTE_BGR)
    println(png.getType)
    for ( x <- 0 until  Xsize ) {
      for (y <- 0 until Ysize) {
        if (png.getRGB(x,y) == -14935012) pngResult.setRGB(x,y,white) // water-white
        if (png.getRGB(x,y) == -16777216) pngResult.setRGB(x,y,yellow) //remain-yellow
        if (png.getRGB(x,y) == -14277082) pngResult.setRGB(x,y,brown) //expand-brown
        if (png.getRGB(x,y) == -15921907) pngResult.setRGB(x,y,pink) //im-pink
      }
    }
    ImageIO.write(pngResult, "png", new File(colorExpand+ ".png"))
  }
}
