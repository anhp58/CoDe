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
//    val configSp: SparkConf = new SparkConf().setAppName("CoDe").setMaster("local[*]")
    val configSp: SparkConf = new SparkConf().setAppName("CoDe").setMaster("spark://192.168.2.23:7077")
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
    val target10:String = "C:\\data\\TOA_VNR20150202_XS_coastal.tif"
    val target25:String = "C:\\data\\TOA_VNR20150202_PXS_Clip_coastal.tif"
    val baseDir:String = "C:\\data\\"
    val expandDir:String = "C:\\data\\Expand\\"
    val remainDir:String = "C:\\data\\Remain\\"

    var refCloud = "Cloud_mask\\TOA_VNR20150117_XS_Clip_coastal_Cloud.tif"
    var refWater = "Water_mask\\TOA_VNR20150117_XS_Clip_coastal_Water.tif"
    var targetCloud = "Cloud_mask\\TOA_VNR20150202_XS_Clip_coastal_Cloud.tif"
    var targetWater = "Water_mask\\TOA_VNR20150202_XS_Clip_coastal_Water.tif"
    // creating directory
    Utilities.createDir(baseDir + "Change")
    Utilities.createDir(baseDir + "ClassificationResult")
    Utilities.createDir(baseDir + "Cloud_mask")
    Utilities.createDir(baseDir + "Difference")
    Utilities.createDir(baseDir + "Expand")
    Utilities.createDir(baseDir + "Feature")
    Utilities.createDir(baseDir + "Remain")
    Utilities.createDir(baseDir + "Water_mask")
    Utilities.createDir(baseDir + "ClassificationResult\\" + "ColorResult")
    //Cloud removal
    /*.
    ...
     */
    CloudRemoval.cloudRemoval(ref10, "TOA_VNR20150117_XS_Clip_coastal_Cloud")
    CloudRemoval.cloudRemoval(target10, "TOA_VNR20150202_XS_Clip_coastal_Cloud")
    println("done removing cloud")
    //Water removal
    /*.
    ...
     */
    WaterRemoval.waterRemoval(ref10, sparkContext, "TOA_VNR20150117_XS_Clip_coastal_Water")
    WaterRemoval.waterRemoval(target10, sparkContext, "TOA_VNR20150202_XS_Clip_coastal_Water")
    println("done removing water")

    val mixName:String = refCloud.subSequence(18, 26).toString + "_" + targetCloud.subSequence(18,26).toString
    println(".........................Expand and Remain detecting.......................")
    //--------------create remain/expand img------------
    /*.
    ...
     */
    val expand10Path = baseDir + "Expand\\" + mixName + "_Expand_XS.tif"
    val remain10Path = baseDir + "Remain\\" + mixName + "_Remain_XS.tif"
    DetectExpandRemain.detectExpandRemain(baseDir + refCloud, baseDir + refWater, baseDir +targetCloud, baseDir + targetWater, expand10Path, remain10Path)
    //convert 10m to 2.5m image
    /*
    ...
     */
    val expand25Path = baseDir + "Expand\\" + mixName + "_Expand_PXS.tif"
    val remain25Path = baseDir + "Remain\\" + mixName + "_Remain_PXS.tif"
    Utilities.convert10mTo25m(target25, expand10Path, remain10Path, expand25Path, remain25Path)
    //Radiometric normalization
    /*.
    ...
     */
    val norDataBand: Array[Array[Double]] = RadiometricNormalization.radiometricNormalization(sparkContext, ref25, remain25Path)
    //create ndfeture
    /*.
    ... for dst image
     */
    val dstndnviFileName: String = Utilities.setMaskFeatureName(mixName, "_Remain_PXS_NDVI")
    val dstnd31FileName: String = Utilities.setMaskFeatureName(mixName, "_Remain_PXS_ND31")
    val dstbrightFileName: String = Utilities.setMaskFeatureName(mixName, "_Remain_PXS_Brightness")
    CreateNDFreatures.createNDFreatures(ref25, sparkContext, norDataBand, dstndnviFileName, dstnd31FileName, dstbrightFileName)
    /*.
    ... for src image
     */
    val srcndnviFileName: String = Utilities.setMaskFeatureName(mixName, "_PXS_NDVI")
    val srcnd31FileName: String = Utilities.setMaskFeatureName(mixName, "_PXS_ND31")
    val srcbrightFileName: String = Utilities.setMaskFeatureName(mixName, "_PXS_Brightness")
    val geoTiffMul: MultibandGeoTiff = GeoTiffReader.readMultiband(ref25)
    val srcDataBandNDFeature:Array[Array[Double]] = Utilities.Open4BandTif(ref25)
    CreateNDFreatures.createNDFreatures(ref25, sparkContext, srcDataBandNDFeature, srcndnviFileName, srcnd31FileName, srcbrightFileName)
    //create difference img
    /*.
    ...
     */
    val differenceFileName = Utilities.setMaskDifferenceName(mixName, "_PXS_NDDifference")
    val brightFileName = Utilities.setMaskDifferenceName(mixName, "_PXS_BrightDifference")
    CreateDifferenceImage.createDifferenceImage(sparkContext, srcndnviFileName, srcnd31FileName, srcbrightFileName, dstndnviFileName, dstnd31FileName, dstbrightFileName, differenceFileName, brightFileName)
    //Detect Change
     /*.
     ...
      */
    val changeImgPath: String = Utilities.setMaskChangeName(mixName, "_PXS_Changed_All")
    DetectChange.detectChange(sparkContext, differenceFileName, brightFileName, changeImgPath)
    // Classification
    /*.
    ...
     */
//    val train:String = "C:\\data\\Train\\train.data"
//    val test:String = "C:\\data\\Test\\test.data"
    val train: String = "hdfs://192.168.2.23:9000/user/pa/train.data"
    val test: String = "hdfs://192.168.2.23:9000/user/pa/test.data"
    var remainClass:String = "C:\\data\\ClassificationResult\\" + mixName + "_Changed_PXS_Classified.tif"
    val expandClass:String = "C:\\data\\ClassificationResult\\" + mixName + "_Expand_PXS_Classified.tif"
    val resultClass: String = "C:\\data\\ClassificationResult\\" + mixName + "_PXS_AggregatedResult.tif"
    Classification.classification(sparkContext, changeImgPath, remainClass, expandClass, train, test, remain25Path, expand25Path)
    Classification.aggregateResult(sparkContext, expandClass, remainClass, resultClass)
    //Color Images
    /*.
    ...
     */
    val colorDir: String = "C:\\data\\ClassificationResult\\ColorResult\\"
    val colorRemain: String = colorDir + "Changed_PXS_Classified"
    val colorExpand: String = colorDir + "Expand_PXS_Classified"
    val colorResult: String = colorDir + "PXS_AggregatedResult"
    Classification.createPNG(remainClass, colorRemain)
    Classification.createPNG(expandClass, colorExpand)
    Classification.createPNG(resultClass, colorResult)
  }
}
