package com.emergency.calls.analysis.uc1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

/**
  * Created by p5103951 on 1/8/17.
  */
object EmergencyCallsAnlysis extends App {

  import org.apache.spark.sql


  // Create Spark Session object
  val spark = SparkSession
    .builder
    .appName("EmergencyCallsAnlysis").master("local")
    .getOrCreate()

  //lat,lng,desc,zip,title,timeStamp,twp,addr,e

  val schema_emergency_ds = StructType(Array(
    StructField("lat", StringType, true),
    StructField("lng", StringType, true),
    StructField("desc", StringType, true),
    StructField("zip", StringType, true),
    StructField("title", StringType, true),
    StructField("timeStamp", StringType, true), // convert to timestamp later
    StructField("twp", StringType, true),
    StructField("addr", StringType, true),
    StructField("e", StringType, true)
  ))


  val schema_zipCode_ds = StructType(Array(
    StructField("zip", StringType, true),
    StructField("city", StringType, true),
    StructField("state", StringType, true),
    StructField("latitude", StringType, true),
    StructField("longitude", StringType, true),
    StructField("timezone", StringType, true),
    StructField("dst", StringType, true)
  ))

  import spark.implicits._

  val emergency_calls_df = spark.read.option("header", true).schema(schema_emergency_ds).csv("/Users/p5103951/IdeaProjects/DataSources/Emergency.csv")
  // emergency_calls_df.show(5)

  emergency_calls_df.createOrReplaceTempView("emergency_table")

  val emergencyTable_result = spark.sql("select * from emergency_table ")
  //emergencyTable_result.show(5)


  val zip_code_df = spark.read.option("header", true).schema(schema_zipCode_ds).csv("/Users/p5103951/IdeaProjects/DataSources/ZipCodes.csv")
  zip_code_df.createOrReplaceTempView("zip_code_table")

  val zip_code_table_result = spark.sql("select * from zip_code_table")
  // zip_code_table_result.show(5)


  val joined_emergency_zip: DataFrame = spark.sql("select et.title,zt.city,zt.state  from emergency_table et, zip_code_table zt where et.zip= zt.zip ")


  //joined_emergency_zip.show(5)

  /**
    * =========================================================
    * Use Case 1: What kind of Problems & Which State (Summary)
    * =========================================================
    */

  val issue_state_tupple = joined_emergency_zip.map(x => (x(0) + " -> " + x(2))).map(item => item).rdd

  val issues_by_state = issue_state_tupple.map(x => (x, 1)).reduceByKey(_ + _).map(item => item).sortByKey(false)


  //issues_stateBy.collect().foreach(println)


  /**
    * =========================================================
    * Use Case 1: What kind of Problems & Which City (Summary)
    * =========================================================
    */
  val issue_city_tupple = joined_emergency_zip.map(x => (x(0) + " -> " + (x(1), x(2)))).map(item => item).rdd
  val issues_by_city = issue_city_tupple.map(x => (x, 1)).reduceByKey(_ + _).map(item => item).sortByKey(false)


  issues_by_city.collect().foreach(println)

}
