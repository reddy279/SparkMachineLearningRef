package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by p5103951 on 4/9/17.
  */
case class customer_info(name: String, bdate: String, address: String, state: String)

object Zipping extends App {


  val spark = SparkSession.builder().master("local").appName("Zipping").getOrCreate()
  val pa: RDD[String] = spark.sparkContext.parallelize(List("a", "c", "e", "m", "p", "k"))


  val details = spark.sparkContext.parallelize(Array(
    customer_info("Sachin", "10-10-1972", "#24, Malad", "Mumbai"),
    customer_info("Sourav", "31-09-1973", "#41, ultadanga", "Kolkata"),
    customer_info("Sehwag", "23-10-1981", "#23, Dwaraka", "Delhi"),
    customer_info("Rahul", "31-12-1971", "#41, Whitefield", "Bangalore")
  ))

  val pzipped: RDD[(String, Long)] = pa.zipWithIndex()


  import spark.sqlContext.implicits._

  val detailsDF: DataFrame = details.toDF()

  detailsDF.show()

  /**
    * (a,0)
    * (c,1)
    * (e,2)
    * (m,3)
    * (p,4)
    * (k,5)
    */


}
