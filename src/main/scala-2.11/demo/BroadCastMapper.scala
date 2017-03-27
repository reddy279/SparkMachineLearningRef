package demo

import org.apache.spark.sql.SparkSession

/**
  * Created by p5103951 on 3/27/17.
  */

/**
  * if you need to join a large table (fact) with relatively small tables (dimensions)
  * i.e. to perform a star-schema join you can avoid sending all data of the large table over the network
  *
  * The following approach allows us not to shuffle the fact table, and to get quite good join performance
  */
object BroadCastMapper extends App {


  val spark = SparkSession.builder().appName("BroadCastMapper").master("local").getOrCreate()

  // Fact table
  val flights = spark.sparkContext.parallelize(List(
    ("SEA", "JFK", "DL", "418", "7:00"),
    ("SFO", "LAX", "AA", "1250", "7:05"),
    ("SFO", "JFK", "VX", "12", "7:05"),
    ("JFK", "LAX", "DL", "424", "7:10"),
    ("LAX", "SEA", "DL", "5737", "7:10")))

  // Dimension table
  val airports = spark.sparkContext.parallelize(List(
    ("JFK", "John F. Kennedy International Airport", "New York", "NY"),
    ("LAX", "Los Angeles International Airport", "Los Angeles", "CA"),
    ("SEA", "Seattle-Tacoma International Airport", "Seattle", "WA"),
    ("SFO", "San Francisco International Airport", "San Francisco", "CA")))

  // Dimension table
  val airlines = spark.sparkContext.parallelize(List(
    ("AA", "American Airlines"),
    ("DL", "Delta Airlines"),
    ("VX", "Virgin America")))


  /**
    * The fact table be very large, while dimension tables are often quite small.
    * Let’s download the dimension tables to the Spark driver, create maps and broadcast them to each worker node:
    */


  val ariportsMap = spark.sparkContext.broadcast(airports.map { case (a, b, c, d) => (a, c) }.collectAsMap)
  // notice we are choosing only 2 fields out of 4.
  val airlinesMap = spark.sparkContext.broadcast(airlines.collectAsMap())


  // Now Map the values

  flights.map({ case (a, b, c, d, e) => (
    ariportsMap.value.get(a).get,
    ariportsMap.value.get(b).get,
    airlinesMap.value.get(c).get, d, e)
  }).foreach(println)


  /**
    * (Seattle,New York,Delta Airlines,418,7:00)
    * (San Francisco,Los Angeles,American Airlines,1250,7:05)
    * (San Francisco,New York,Virgin America,12,7:05)
    * (New York,Los Angeles,Delta Airlines,424,7:10)
    * (Los Angeles,Seattle,Delta Airlines,5737,7:10)
    */
}

