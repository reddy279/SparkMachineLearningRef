package demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by p5103951 on 3/27/17.
  */
/**
  * SELECT store, product, SUM(amount), MIN(amount), MAX(amount), SUM(units)
  * FROM sales
  * GROUP BY store, product
  */
object MultiColReduceKey extends App {


  val spark = SparkSession.builder().appName("MultiColReduceKey").master("local").getOrCreate()

  val sales = spark.sparkContext.parallelize(List(
    ("West", "Apple", 2.0, 10),
    ("West", "Apple", 3.0, 15),
    ("West", "Orange", 5.0, 15),
    ("South", "Orange", 3.0, 9),
    ("South", "Orange", 6.0, 18),
    ("East", "Milk", 5.0, 5)))

  sales.map({ case (store, product, amount, units) => ((store, product), (amount, amount, amount, units)) })
    .reduceByKey((x, y) =>
      (x._1 + y._1, math.min(x._2, y._2), math.max(x._3, y._3), x._4 + y._4)).foreach(println)


  /**
    * ((West,Orange),(5.0,5.0,5.0,15))
    * ((East,Milk),(5.0,5.0,5.0,5))
    * ((South,Orange),(9.0,3.0,6.0,27))
    * ((West,Apple),(5.0,2.0,3.0,25))
    */

}
