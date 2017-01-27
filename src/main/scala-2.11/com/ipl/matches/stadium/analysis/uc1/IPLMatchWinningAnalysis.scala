package com.ipl.matches.stadium.analysis.uc1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by p5103951 on 1/8/17.
  * ====================================================================
  * USE CASE : Probablity/Chance of Winning the Match by Batting First on a particular Stadium.
  * ====================================================================
  * Data Column Name :
  * id	Season	city	date	team1	team2	toss_winner	toss_decision	result	dl_applied	winner	win_by_runs	win_by_wickets	man_of_the_match	Venue	empire	Commentator
  * 1
  */
object IPLMatchWinningAnalysis extends App {


  // Create Spark Session object
  val spark = SparkSession
    .builder
    .appName("IPLMatchWinningAnalysis").master("local")
    .getOrCreate()

  // Read the original Dataset
  val matchDataset: RDD[String] = spark.sparkContext.textFile("/Users/p5103951/IdeaProjects/SparkMachineLearningRef/src/main/resources/IPL_Matches_small.csv")

  // Filter the records which do not have 19 fileds
  val filterBadRecords: RDD[Array[String]] = matchDataset.map(x => x.split(",")).filter(line => line.length < 19)

  // Extract only toss_decision, won_by_runs, won_by_wickets, venue for our analysis
  val extractNeededFields: RDD[(String, String, String, String)] = filterBadRecords.map(x => (x(7), x(11), x(12), x(14)))

  // filter out the columns which are having won_by_runs value as 0 so that we can get the teams which won by batting first
  val firstBattingWon = extractNeededFields.filter(x => x._2 != "0").map(x => (x._4, 1)).reduceByKey(_ + _)


  // Inorder to find the winning percentage at each stadium, we need to calculate the total matches played at each stadium.

  val totalMatchesAtEachStadium = filterBadRecords.map(x => (x(14), 1)).reduceByKey(_ + _)

  //Calculate the winning percentage based on the stadium where they won and number of matched played
  //Ex:(Stadium, Number of winnings*100/Number of Matches played)

  firstBattingWon.join(totalMatchesAtEachStadium).map(x =>
    (x._1, (x._2._1 * 100 / x._2._2))).map(item => item.swap).sortByKey(false).collect().foreach(println)


}
