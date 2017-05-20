package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by p5103951 on 4/9/17.
  */
object CustomFielding extends App {


  val spark = SparkSession.builder().appName("CustomFielding").master("local").getOrCreate()
  import spark.sqlContext.implicits._
  val data:DataFrame = spark.sparkContext.parallelize(List(
    ("Sachin", "10-10-1972", "#24, Malad", "Mumbai"),
    ("Sourav", "31-09-1973", "#41, ultadanga", "Kolkata"),
    ("Sehwag", "23-10-1981", "#23, Dwaraka", "Delhi"),
    ("Rahul", "31-12-1971", "#41, Whitefield", "Bangalore")
  )).toDF("name","dbirth","Address","city")


   data.show()



   // data.map(case(name:String, bd:String, add:String,city:String)=>(name,bd,add,city))





}
