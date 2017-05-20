package demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by p5103951 on 3/15/17.
  */

object WordCount extends App {

  object WordCount {
    def main(args: Array[String]) {
      val inputFile = args(0)
      val outputFile = args(1)
      val conf = new SparkConf().setAppName("wordCount")
      val sc = new SparkContext(conf)

      val input = sc.textFile(inputFile)

      val words = input.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey (_+_)

      //val words = input.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey { case (x, y) => x + y }

      words.saveAsTextFile(outputFile)
    }
  }

}
