package main

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.{File, FileInputStream}
import javax.imageio.ImageIO

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
  def classification (sc:SparkContext, changeImgPath: String, remainClass:String, expandClass: String, trainPath: String, testPath: String, remainPath: String, expandPath: String): Unit = {
    val spark = SparkSession
      .builder
      .appName("LinearSVCExample")
      .getOrCreate()
    val training = spark.read.format("libsvm").load(trainPath)
    val testing = spark.read.format("libsvm").load(testPath)
    val lsvc = new LinearSVC().setWeightCol("classWeightCol").setLabelCol("label").setFeaturesCol("features")
    val lsvcModel = lsvc.fit(Utilities.balanceDataset(training))

    //classification remain data
    val remainData: Array[Array[Double]] = Utilities.Open4BandTif(remainPath)
    //
    //classification expand data
    val expandData: Array[Array[Double]] = Utilities.Open4BandTif(expandPath)
    //
    val changeData: Array[Double] = Utilities.Open1BandTif(changeImgPath)

    val Xsize = Utilities.getColMultiBand(remainPath)
    val Ysize = Utilities.getRowMultiBand(remainPath)

    val imgSize = Ysize*Xsize
    var classRemainArr: Array[Double] = Array.ofDim[Double](Xsize*Ysize)
    var classExpandArr: Array[Double] = Array.ofDim[Double](Xsize*Ysize)
    //classification remain
    //
    var remainDataDF: DataFrame = spark
      .createDataFrame(Seq(LabeledPoint(0.0, Vectors.sparse(4, Array(0, 1, 2, 3), Array(remainData(0)(0), remainData(1)(0), remainData(2)(0), remainData(3)(0))))))
      .toDF()
    //classification expand
    var expandDataDF: DataFrame = spark
      .createDataFrame(Seq(LabeledPoint(0.0, Vectors.sparse(4, Array(0, 1, 2, 3), Array(expandData(0)(0), expandData(1)(0), expandData(2)(0), expandData(3)(0))))))
      .toDF()
    //
    for (i <- 0 until imgSize) {
      if (changeData(i) == 1) {
        if (!remainData(0)(i).isNaN) {
          remainDataDF = spark
            .createDataFrame(Seq(LabeledPoint(0.0, Vectors.sparse(4, Array(0, 1, 2, 3), Array(remainData(0)(i), remainData(1)(i), remainData(2)(i), remainData(3)(i))))))
            .toDF()
//          classRemainArr(i) = lsvcModel.transform(remainDataDF).select("prediction").collectAsList().get(0)(0).toString.toDouble
          classRemainArr(i) = lsvcModel.transform(remainDataDF).select("prediction").first().getDouble(0)
        }
      } else classRemainArr(i) = 3
      // classification expand
      if (expandData(0)(i) > 0) {
        if (!expandData(0)(i).isNaN) {
          expandDataDF = spark
            .createDataFrame(Seq(LabeledPoint(0.0, Vectors.sparse(4, Array(0, 1, 2, 3), Array(expandData(0)(i), expandData(1)(i), expandData(2)(i), expandData(3)(i))))))
            .toDF()
//          classExpandArr(i) = lsvcModel.transform(expandDataDF).select("prediction").collectAsList().get(0)(0).toString.toDouble
          classExpandArr(i) = lsvcModel.transform(expandDataDF).select("prediction").first().getDouble(0)
        }
      } else classExpandArr(i) = 3
      //
    }
    //classification remain tiff
    val geoTiffMulR: MultibandGeoTiff = GeoTiffReader.readMultiband(remainPath)
    val tiffRemain = DoubleArrayTile(classRemainArr, Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(tiffRemain, geoTiffMulR.extent, geoTiffMulR.crs).write(remainClass)
    //
    //classification expand tiff
    val geoTiffMulE: MultibandGeoTiff = GeoTiffReader.readMultiband(expandPath)
    val tiffExpand = DoubleArrayTile(classExpandArr, Xsize, Ysize).convert(FloatCellType)
    SinglebandGeoTiff(tiffExpand, geoTiffMulE.extent, geoTiffMulE.crs).write(expandClass)

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
    val black = Color.black.getRGB
    val yellow = Color.yellow.getRGB
    val blue = Color.blue.getRGB
    val brown = new Color(153,76,0).getRGB
    val pink = new Color(255, 20,147).getRGB
    println("-----redering png----------")
    //read tiff file
    val img:Mat  = imread(tifImg,COLOR_GRAY2BGR)
    //write png file
    imwrite(colorExpand + ".png", img)
    //read png file
    val png: BufferedImage  = ImageIO.read(new FileInputStream(colorExpand + ".png"))
    val pngResult: BufferedImage = new BufferedImage(png.getWidth, png.getHeight, BufferedImage.TYPE_3BYTE_BGR)
    for ( x <- 0 until  Xsize ) {
      for (y <- 0 until Ysize) {
        if (png.getRGB(x,y) == -14935012) pngResult.setRGB(x,y,black) // water-white
        if (png.getRGB(x,y) == -16777216) pngResult.setRGB(x,y,brown) //remain-yellow
        if (png.getRGB(x,y) == -14277082) pngResult.setRGB(x,y,yellow) //expand-brown
        if (png.getRGB(x,y) == -15921907) pngResult.setRGB(x,y,pink) //im-pink
      }
    }
    ImageIO.write(pngResult, "png", new File(colorExpand+ ".png"))
  }
}
