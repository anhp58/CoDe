package main

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff._
import geotrellis.raster._
import geotrellis.vector.ProjectedExtent

object Main {
  def main(args: Array[String]): Unit = {
    println(".........starting..............")
    val configSp: SparkConf = new SparkConf().setAppName("CoDe").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(configSp)

    println("................done creating spark context.............")

    // commit 2 RDD
    //    val hdfsPrefix:String = "hdfs://"
    //    val namenodeIP:String = "192.168.2.23"
    //    val hadoopPort:String = "9000"
    //    val hadoopUrl:String = hdfsPrefix + namenodeIP + ":" + hadoopPort
    //    val fileName:String = "TOA_VNR20150117_PXS_Clip_coastal.tif"
    //    val userName = "pa/"
    //    val folderName = "IMG/"
    val ref10:String = "C:\\data\\TOA_VNR20150117_XS_coastal.tif"
    val ref25:String = "C:\\data\\TOA_VNR20150117_PXS_Clip_coastal.tif"
    val baseDir:String = "C:\\data\\"

    var refCloud = "Cloud_mask\\TOA_VNR20150117_PXS_Clip_coastal_Cloud.tif"
    var refWater = "Water_mask\\TOA_VNR20150117_PXS_Clip_coastal_Water.tif"
    var targetCloud = "Cloud_mask\\TOA_VNR20150202_PXS_Clip_coastal_Cloud.tif"
    var targetWater = "Water_mask\\TOA_VNR20150202_PXS_Clip_coastal_Water.tif"

    val target10:String = "C:\\data\\TOA_VNR20150202_XS_coastal.tif"
    val target25:String = "C:\\data\\TOA_VNR20150202_PXS_Clip_coastal.tif"
//    println(expandFileName)
//    CloudRemoval.cloudRemoval(ref10)
//    CloudRemoval.cloudRemoval(ref25)
////    println("....................cloud mask id created..............")
//    WaterRemoval.waterRemoval(ref10, sparkContext)
//    WaterRemoval.waterRemoval(ref25, sparkContext)
//    println("....................water mask is created..............")
    println(".........................Expand and Remain detecting.......................")
    //create remain/expand img
    val expandFileName: String = Utilities.setMaskExpandName(refCloud.subSequence(18, 26).toString + "_" + targetCloud.subSequence(18,26).toString)
    val remainFileName: String = Utilities.setMaskRemainName(refCloud.subSequence(18, 26).toString + "_" + targetCloud.subSequence(18,26).toString)
    val remainDataBand: Array[Array[Double]] = DetectExpandRemain.detectExpandRemain(target25, baseDir + refCloud,baseDir + refWater, baseDir + targetCloud, baseDir + targetWater)(1)
    val expandDataBand: Array[Array[Double]] = DetectExpandRemain.detectExpandRemain(target25, baseDir + refCloud,baseDir + refWater, baseDir + targetCloud, baseDir + targetWater)(0)
    //Radiometric normalization
//    val norDataBand: Array[Array[Double]] = RadiometricNormalization.radiometricNormalization(sparkContext, ref25, remainDataBand)
    //create ndfeture for dst img
    val dstndnviFileName: String = Utilities.setMaskFeatureName(refCloud.subSequence(18, 26).toString + "_" + targetCloud.subSequence(18,26).toString, "_Remain_PXS_NDVI")
    val dstnd31FileName: String = Utilities.setMaskFeatureName(refCloud.subSequence(18, 26).toString + "_" + targetCloud.subSequence(18,26).toString, "_Remain_PXS_ND31")
    val dstbrightFileName: String = Utilities.setMaskFeatureName(refCloud.subSequence(18, 26).toString + "_" + targetCloud.subSequence(18,26).toString, "_Remain_PXS_Brightness")
//    CreateNDFreatures.createNDFreatures(ref25, sparkContext, norDataBand, dstndnviFileName, dstnd31FileName, dstbrightFileName)
    //create ndfeature for src img
    val srcndnviFileName: String = Utilities.setMaskFeatureName(refCloud.subSequence(18, 26).toString + "_" + targetCloud.subSequence(18,26).toString, "_PXS_NDVI")
    val srcnd31FileName: String = Utilities.setMaskFeatureName(refCloud.subSequence(18, 26).toString + "_" + targetCloud.subSequence(18,26).toString, "_PXS_ND31")
    val srcbrightFileName: String = Utilities.setMaskFeatureName(refCloud.subSequence(18, 26).toString + "_" + targetCloud.subSequence(18,26).toString, "_PXS_Brightness")
    val geoTiffMul: MultibandGeoTiff = GeoTiffReader.readMultiband(ref25)
    val srcDataBandNDFeature:Array[Array[Double]] = Utilities.Open4BandTif(ref25)
//    CreateNDFreatures.createNDFreatures(ref25, sparkContext, srcDataBandNDFeature, srcndnviFileName, srcnd31FileName, srcbrightFileName)
    //create difference img
    val differenceFileName = Utilities.setMaskDifferenceName(refCloud.subSequence(18, 26).toString + "_" + targetCloud.subSequence(18,26).toString, "_PXS_NDDifference")
    val brightFileName = Utilities.setMaskDifferenceName(refCloud.subSequence(18, 26).toString + "_" + targetCloud.subSequence(18,26).toString, "_PXS_BrightDifference")
//    CreateDifferenceImage.createDifferenceImage(sparkContext, srcndnviFileName, srcnd31FileName, srcbrightFileName, dstndnviFileName, dstnd31FileName, dstbrightFileName, differenceFileName, brightFileName)
    //Detect Change
    val changeImgPath: String = Utilities.setMaskChangeName(refCloud.subSequence(18, 26).toString + "_" + targetCloud.subSequence(18,26).toString, "_PXS_Changed_All")
    DetectChange.detectChange(sparkContext, differenceFileName, brightFileName, changeImgPath)
  }
}
