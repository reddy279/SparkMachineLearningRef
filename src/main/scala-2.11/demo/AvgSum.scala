package demo

import org.apache.commons.lang._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object AvgSum extends App {


  val spark = SparkSession.builder().master("local").appName("AvgSum").getOrCreate()
  val data: RDD[String] = spark.sparkContext.textFile("/Users/p5103951/IdeaProjects/SparkMachineLearningRef/src/main/resources/sampleText.txt")
  val meanValue = data.flatMap(x => x.split(" ")).filter(a => StringUtils.isNumeric(a)).map(x => x.toInt).mean()
  println("Mean " + meanValue)


  val myRange:Range = 1 to 5

}
