name := "SparkMachineLearningRef"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % "2.11.8",
  "org.apache.spark" %% "spark-core" % "2.0.2" ,
  "org.apache.spark" %% "spark-sql" % "2.0.1",
  "org.apache.spark" %% "spark-streaming" % "2.0.1" ,
  "org.apache.spark" %% "spark-mllib" % "2.0.1",
  "org.apache.commons" %% "commons-lang3" % "3.5"

)