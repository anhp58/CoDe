package main

import sys.process._


object Classification {
  def classificationRemain (changeImgPath: String, outname:String, trainPath: String, testPath: String, rb0:String , rb1: String, rb2: String, rb3:String): Unit = {
    println("-----------start classification remain----------")
    val command = "python F:\\CoDe\\src\\main\\scala\\main\\classification.py " + rb0 + " " + rb1 + " " + rb2 + " " + rb3 + " " + changeImgPath + " " + outname + " " + trainPath + " " + testPath
    command.!
    println(command)
  }
  def classificationExpand (remainImgPath: String, outname:String, trainPath: String, testPath: String, eb0:String , eb1: String, eb2: String, eb3:String): Unit = {
    println("-----------start classification expand----------")
    val command = "python F:\\CoDe\\src\\main\\scala\\main\\classification.py " + eb0 + " " + eb1 + " " + eb2 + " " + eb3 + " " + remainImgPath + " " + outname + " " + trainPath + " " + testPath
    command.!
    println(command)
  }
}
