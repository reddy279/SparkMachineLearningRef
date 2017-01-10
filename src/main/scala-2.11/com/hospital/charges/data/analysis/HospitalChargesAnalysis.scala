package com.hospital.charges.data.analysis

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by p5103951 on 1/9/17.
  */
object HospitalChargesAnalysis extends App {


  val spark = SparkSession.builder().appName("HospitalChargesAnalysis").master("local").getOrCreate()


  val dataFrame = spark.read.option("header", true).option("inferSchema", true).csv("/Users/p5103951/IdeaProjects/DataSources/inpatientCharges.csv")
  //dataFrame.show(5)
  //dataFrame.printSchema()

  /**
    * UC1: Average Amounts Changed per State
    */
  val averageCharges_by_state: DataFrame = dataFrame.groupBy("ProviderState").avg("AverageCoveredCharges")
 // averageCharges_by_state.show()

  /**
    * UC1: Average Total Payments Amounts Changed per State
    */

  val average_total_payments = dataFrame.groupBy("ProviderState").avg("AverageTotalPayments")
 // average_total_payments.show()


  dataFrame.groupBy(("ProviderState"),("DRGDefinition")).sum("TotalDischarges").show()

  //.orderBy(desc(sum("TotalDischarges").toString)).show
}
