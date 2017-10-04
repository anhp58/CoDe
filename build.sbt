import sbt._

name := "CoDe"

version := "0.1"

scalaVersion := "2.11.11"

val nd4jVersion = "0.7.2"

libraryDependencies += "org.nd4j" % "nd4j-native-platform" % nd4jVersion

libraryDependencies += "org.locationtech.geotrellis" %% "geotrellis-raster" % "1.0.0"

libraryDependencies += "org.nd4j" %% "nd4s" % nd4jVersion

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-spark"  % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-s3"     % "1.0.0", // now we can use Amazon S3!
  "org.apache.spark"            %% "spark-core"        % "2.1.0" % "provided",
  "org.scalatest"               %% "scalatest"         % "3.0.0" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies += "org.bytedeco" % "javacpp" % "1.3.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-mllib" % "2.2.0"
)
