package main

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff._
import geotrellis.raster._
import geotrellis.vector.ProjectedExtent

object Main {
  def main(args: Array[String]): Unit = {
    val configSp: SparkConf = new SparkConf().setAppName("CoDe").setMaster("local[*]")
    // sparkContext is metadata about spark cluster used to creating RDD
    val sparkContext: SparkContext = new SparkContext(configSp)

    println("done creating spark context")

    // commit 2 RDD
    //    val hdfsPrefix:String = "hdfs://"
    //    val namenodeIP:String = "192.168.2.23"
    //    val hadoopPort:String = "9000"
    //    val hadoopUrl:String = hdfsPrefix + namenodeIP + ":" + hadoopPort
    //    val fileName:String = "TOA_VNR20150117_PXS_Clip_coastal.tif"
    //    val userName = "pa/"
    //    val folderName = "IMG/"
    val pathSingleBand:String = "C:\\data\\TOA_VNR20150117_XS_coastal.tif"
    val pathMultipleBand:String = "C:\\data\\TOA_VNR20150117_PXS_Clip_coastal.tif"
    CloudRemoval.cloudRemoval(pathMultipleBand)
    WaterRemoval.waterRemoval(pathMultipleBand)
    println("create water mask..............")
  }
}
