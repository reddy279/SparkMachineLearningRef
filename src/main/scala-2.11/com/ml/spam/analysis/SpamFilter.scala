package com.ml.spam.analysis


import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Shashidhar Reddy  on 1/31/17.
  */


/**
  *
  * The Following usecase implementation determines if the mail we received is a Spam mail or Ham mail.
  */
object SpamFilter extends App {

  val spark = SparkSession.builder.master("local").appName("SpamFiltering").getOrCreate()


  val spam_mails: RDD[String] = spark.sparkContext.textFile("/Users/p5103951/IdeaProjects/SparkMachineLearningRef/src/main/resources/spam_file")

  val ham_mails: RDD[String] = spark.sparkContext.textFile("/Users/p5103951/IdeaProjects/SparkMachineLearningRef/src/main/resources/ham_file")


  /** Create HashingTF  to map emails text to Vectors of 10000 features.
    * This is also known as HasingTrick , is a fast and space-efficient way of vectorizing features.
    * Ie : Turning arbitrary features into Indeces in a Vector or Matrix.
    * */

  val hasingTf = new HashingTF(numFeatures = 10000)


  /**
    * Each email is split into words , and each word is mapped to one feature.
    */

  val spamFeatures = spam_mails.map(email => hasingTf.transform(email.split(" ")))
  val hamFeatures = ham_mails.map(email => hasingTf.transform(email.split(" ")))

  /**
    * Spam Filtering is kind of Supervised learning , we need to provide labled data to the application.
    * Labeled Data typically consists of a bag of multidimentional feature vectores.
    * A labled vector is a local vector, either Dense or Sparse , associated with label / response .
    *
    * Labled points are used in Supervisied Learning Algortihems .
    *
    * Create LabledPoint datasets for positive(Spam), negative(ham) examples.
    */
  val positiveExamples: RDD[LabeledPoint] = spamFeatures.map(features => LabeledPoint(1, features))
  val negativeExamples: RDD[LabeledPoint] = hamFeatures.map(features => LabeledPoint(0, features))
  // positiveExamples.foreach(println) // run it to see the vector and its features mapped

  /**
    * Union both the Spam and Ham Labled Datasets.
    */
  val trainingData: RDD[LabeledPoint] = positiveExamples.union(negativeExamples)
  /**
    * Cache the dats as Logistic Regression is an Iterative Algorithm.
    */
  trainingData.cache()


  /**
    * Create Logistic Regression Linear which uses LBFGS opitmizer.
    *
    */
  val logisticLearner = new LogisticRegressionWithLBFGS()

  /**
    * Run the actual learning algorithm on the training data.
    */
  val model = logisticLearner.run(trainingData)

  /**
    * Test on a positive example (spam) and a negative one (normal).
    *
    * Apply the HashingTF  feature transformation used on the traning data.
    */
  val posTest = hasingTf.transform(" YES-440 insurance FREE plan which change your life ...".split(" "))
  val negTest = hasingTf.transform("hi sorry yaar i forget tell you i cant come today".split(" "))


  /**
    * Now apply learning model to predict the spam/ ham for new emails.
    */

  println("Prediction for positive test example: " + model.predict(posTest))
  println("Prediction for negative test example: " + model.predict(negTest))


  /**
    * Out put :
    * Prediction for positive test example: 1.0     // Confirms the text we tested has  Spam content
    * Prediction for negative test example: 0.0     // Confirms the text we tested has no Spam content
    *
    * ========================================================================================
    **/


  /**
    * ========================================================================================
    * Determine the accuracy of the models with different algorithms
    * 1. Logistic Regression
    * 2. Naive Byes
    * ========================================================================================
    */

  val Array(training, test) = trainingData.randomSplit(Array(0.6, 0.4))

  val logisticPredictionLable = test.map(x => (model.predict(x.features), x.label))

  /**
    * Using Logistic Regression :
    * -------------------------
    * Accuracy can be calculated by taking the matching terms from the both the traning and test data
    */

  val logisticAccuracy = 1.0 * logisticPredictionLable.filter(x => x._1 == x._2).count() / training.count()
  println("logisticAccuracy  :: " + logisticAccuracy) //0.6476500147797812 ,0.6360434399765189


  /**
    * Out put :
    *
    * The Accuracy of this Logistic Regression model based on the training and test data is 64%
    */


  /**
    * Using Naive Base  :
    * -------------------------
    * Accuracy can be calculated by taking the matching terms from the both the traning and test data
    */


  val naiveModel = NaiveBayes.train(training, 1.0)
  val naivePredictionLabel = test.map(x => (naiveModel.predict(x.features), x.label))

  val naiveAccuracy = 1.0 * naivePredictionLabel.filter(x => x._1 == x._2).count() / training.count()

  println("naiveAccuracy  :: " + naiveAccuracy) //0.6436884512085944


  /**
    * Out put :
    *
    * The Accuracy of this Naive Base  model based on the training and test data is 64%
    */


  /**
    * Final Decision :
    * The accuracy of the Spam Filtering Model is almost same with  both the algorithms.
    */


  spark.stop();

}
