package com.uber.core.uc1

import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by Shashidhar Reddy S on 1/2/17.
  * ====================================================================
  * USE CASE : Find the days on which each Uber Basement has more trips.
  * ====================================================================
  *
  */

object UberCaseWeeklyDemand extends App {


  // Create Spark Session object
  val spark = SparkSession
    .builder
    .appName("UberCaseWeeklyDemand").master("local")
    .getOrCreate()

  // Read the Dataset from the source.
  val uberDataSet: RDD[String] = spark.sparkContext.textFile("/Users/p5103951/IdeaProjects/SparkMachineLearningRef/src/main/resources/uberData_small.txt")
  // Reader the Header Row
  val header = uberDataSet.first();


  // Create Date Forment
  val dateFormat = new java.text.SimpleDateFormat("MM/dd/yyyy")

  val weekDays: Array[String] = Array("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")

  //Remove the Header part from the given DataSource
  val dataSetWithNoHeader: RDD[String] = uberDataSet.filter(line => line != header)

  // Split the DS Records and extract the fields required.f(0), f(1) and f(3)
  val fieldSplit: RDD[(String, Date, String)] = dataSetWithNoHeader.map(line => line.split(",")).map {
    f => (f(0), dateFormat.parse(f(1)), f(3))

  }

  // Create a Tuple for Basement as Key and remianing two feilds and values.
  // Ex :(Key, Value) = (B02765 Mon,NumberOfTrips)

  val fieldsPairingAndDayFormat: RDD[(String, Int)] = fieldSplit.map(x => (x._1 + " " + weekDays(x._2.getDay), x._3.toInt))


  // Prints the result on the console. The sorting set on tbe basement
  val result: Unit = fieldsPairingAndDayFormat.reduceByKey(_ + _).sortByKey().collect().foreach(println)


}
