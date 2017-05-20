package demo

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.row_number

import org.apache.spark.sql.SparkSession

/**
  * Created by p5103951 on 4/13/17.
  */
object WindowAggr extends App {


  val spark = SparkSession.builder().appName("WindowAggr").master("local").getOrCreate()

  import spark.sqlContext.implicits._

  val df = spark.sparkContext.parallelize(Seq(
    (0, "cat26", 30.9), (0, "cat13", 22.1), (0, "cat95", 19.6), (0, "cat105", 1.3),
    (1, "cat67", 28.5), (1, "cat4", 26.8), (1, "cat13", 12.6), (1, "cat23", 5.3),
    (2, "cat56", 39.6), (2, "cat40", 29.7), (2, "cat187", 27.9), (2, "cat68", 9.8),
    (3, "cat8", 35.6))).toDF("Hour", "Category", "TotalValue")

  /**
    * +----+--------+----------+
    * |Hour|Category|TotalValue|
    * +----+--------+----------+
    * |   0|   cat26|      30.9|
    * |   0|   cat13|      22.1|
    * |   0|   cat95|      19.6|
    * |   0|  cat105|       1.3|
    * |   1|   cat67|      28.5|
    * |   1|    cat4|      26.8|
    * |   1|   cat13|      12.6|
    * |   1|   cat23|       5.3|
    * |   2|   cat56|      39.6|
    * |   2|   cat40|      29.7|
    * |   2|  cat187|      27.9|
    * |   2|   cat68|       9.8|
    * |   3|    cat8|      35.6|
    * +----+--------+----------+
    *
    */


  val windowFunc: WindowSpec = Window.partitionBy($"Hour").orderBy($"TotalValue".desc)
  val maxFun = df.withColumn("aggCol", row_number.over(windowFunc)).where($"aggCol" === 1).drop("aggCol")
  maxFun.show()

}
