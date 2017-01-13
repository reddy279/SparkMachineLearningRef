package com.uber.ml.ml.cluster.analysis.uc1


import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, _}

/**
  * Created by p5103951 on 1/10/17.
  *
  * USE CASE : Discover the clusters of Uber data based on the longitude and latitude.
  *
  *
  */
object UberCluster extends App {

  // Create Spark Session object
  val spark = SparkSession
    .builder
    .appName("UberCluster").master("local")
    .getOrCreate()


  val dataset_schema = StructType(Array(
    StructField("dt", TimestampType, true),
    StructField("lat", DoubleType, true),
    StructField("lon", DoubleType, true),
    StructField("base", StringType, true)
  ))

  // Read the DataSet which creates the DataFrame
  val uber_ds_df = spark.read.option("header", false).schema(dataset_schema).csv("/Users/p5103951/IdeaProjects/DataSources/uber.csv")

  // Cache the results for future use.
  uber_ds_df.cache()


  // defining feature array which would be used by Machine Learning algorithms.
  // These features are transformed into Feature Vectors of the numbers  .


  val feature_cols= Array("lat", "lon")

  // Set the input column names to the feature Vectore


  val assembler = new VectorAssembler().setInputCols(feature_cols).setOutputCol("features")

  //new dataframe with all the features in the vector column
  val transferred_df= assembler.transform(uber_ds_df)

  // randomly splitting the data for traning and test
  val Array(trainingData, testData)=  transferred_df.randomSplit(Array(0.7,0.3),5043)

  val kmean= new KMeans().setK(8).setFeaturesCol("features").setPredictionCol("predictions")

  //building the model
  val model = kmean.fit(transferred_df)

  //save the oput put to path
  //model.save("")

  model.clusterCenters.foreach(println)







}
