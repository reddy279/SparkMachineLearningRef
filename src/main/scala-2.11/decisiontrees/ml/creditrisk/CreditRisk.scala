package decisiontrees.ml.creditrisk

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Shashidhar Reddy Sudi on 2/4/17.
  */

/**
  * Random Forest Learning Method for Classification and Regression
  *
  * Ensemble Learning Alogorithm combine multiple machine learning algorithms to obtian a better model.
  * Random Forest is a popular ensemble learning method for Classfication and Regression.
  * This builds a model consisting of multiple decision trees, bases on difference subsets of data
  * at the traning stage.  Prediction are made by combining the output from all of the trees which reduces the variance, and
  * improves the prededctive accuracy.
  *
  * For Random Forest Classification each tree's prediction is counted as vote for one class.
  * The label is predicted to be the class which receives the most votes.
  *
  */


object CreditRisk extends App {

  /**
    * Defining Credit Class
    */

  case class Credit(
                     creditability: Double, balance: Double, duration: Double, history: Double, purpose: Double,
                     amount: Double, savings: Double, employment: Double, instPrecentage: Double,
                     sexMarried: Double, guarantors: Double, residenceDuration: Double, assets: Double,
                     age: Double, concCredit: Double, apartment: Double, credits: Double, occupation: Double,
                     dependents: Double, hasPhone: Double, foreign: Double
                   )

  /**
    * The function below parse a line from the file into Credit Class .
    * Subtracts 1 from some categorical values so that they all consistently starts with 0.
    */

  def parseCredit(line: Array[Double]): Credit = {

    Credit(
      line(0), line(1) - 1, line(2), line(3), line(4), line(5),
      line(6) - 1, line(7) - 1, line(8), line(9) - 1, line(10) - 1,
      line(11) - 1, line(12) - 1, line(13), line(14) - 1, line(15) - 1,
      line(16) - 1, line(17) - 1, line(18) - 1, line(19) - 1, line(20) - 1
    )

  }

  /**
    * the function bleow transform an RDD[Strings] to RDD[Double]
    */

  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {

    rdd.map(_.split(",")).map(_.map(_.toDouble))

  }

  /**
    * ========================================================================
    * Spark DataFrame and Spark SQL Concepts --Begin
    * ========================================================================
    */
  /**
    * Load the file and creata a DataFrame.
    */


  val spark = SparkSession.builder().appName("CreaditRisk").master("local").getOrCreate()
  val creditData: RDD[String] = spark.sparkContext.textFile("/Users/p5103951/IdeaProjects/SparkMachineLearningRef/src/main/resources/CreditData.csv")

  import spark.sqlContext.implicits._

  val creditDF: DataFrame = parseRDD(creditData).map(parseCredit).toDF().cache()
  creditDF.registerTempTable("credit")
  //creditDF.printSchema()
  //creditDF.show(5)
  /**
    * +-------------+-------+--------+-------+-------+------+-------+----------+--------------+----------+---------+-----------------+------+----+----------+---------+-------+----------+----------+--------+-------+
    * |creditability|balance|duration|history|purpose|amount|savings|employment|instPrecentage|sexMarried|guarantors|residenceDuration|assets| age|concCredit|apartment|credits|occupation|dependents|hasPhone|foreign|
    * +-------------+-------+--------+-------+-------+------+-------+----------+--------------+----------+---------+-----------------+------+----+----------+---------+-------+----------+----------+--------+-------+
    * |          1.0|    0.0|    18.0|    4.0|    2.0|1049.0|    0.0|       1.0|           4.0|       1.0|      0.0|              3.0|   1.0|21.0|       2.0|      0.0|    0.0|       2.0|       0.0|     0.0|    0.0|
    * |          1.0|    0.0|     9.0|    4.0|    0.0|2799.0|    0.0|       2.0|           2.0|       2.0|      0.0|              1.0|   0.0|36.0|       2.0|      0.0|    1.0|       2.0|       1.0|     0.0|    0.0|
    * |          1.0|    1.0|    12.0|    2.0|    9.0| 841.0|    1.0|       3.0|           2.0|       1.0|      0.0|              3.0|   0.0|23.0|       2.0|      0.0|    0.0|       1.0|       0.0|     0.0|    0.0|
    * |          1.0|    0.0|    12.0|    4.0|    0.0|2122.0|    0.0|       2.0|           3.0|       2.0|      0.0|              1.0|   0.0|39.0|       2.0|      0.0|    1.0|       1.0|       1.0|     0.0|    1.0|
    * |          1.0|    0.0|    12.0|    4.0|    0.0|2171.0|    0.0|       2.0|           4.0|       2.0|      0.0|              3.0|   1.0|38.0|       0.0|      1.0|    1.0|       1.0|       0.0|     0.0|    1.0|
    * +-------------+-------+--------+-------+-------+------+-------+----------+--------------+----------+---------+-----------------+------+----+----------+---------+-------+----------+----------+--------+-------+
    * only showing top 5 rows
    *
    */


  /**
    * Following computes Statistics for Balance.
    */
  //creditDF.describe("balance").show()
  /**
    * +-------+------------------+
    * |summary|           balance|
    * +-------+------------------+
    * |  count|              1000|
    * |   mean|             1.577|
    * | stddev|1.2576377271108938|
    * |    min|               0.0|
    * |    max|               3.0|
    * +-------+------------------+
    */

  /**
    * Following computes the average balance by creditability
    */
  // creditDF.groupBy("creditability").avg("balance").show()

  /**
    * +-------------+------------------+
    * |creditability|      avg(balance)|
    * +-------------+------------------+
    * |          0.0|0.9033333333333333|
    * |          1.0|1.8657142857142857|
    * +-------------+------------------+
    */

  /**
    * One in SqlContext
    */
  spark.sqlContext.sql(" SELECT creditability ,avg(balance) as avgBalance , avg(amount) as avgAmount, avg(duration) as avgDuration" +
    " FROM credit GROUP BY creditability")

  /**
    * +-------------+------------------+------------------+------------------+
    * |creditability|        avgBalance|         avgAmount|       avgDuration|
    * +-------------+------------------+------------------+------------------+
    * |          0.0|0.9033333333333333|3938.1266666666666|             24.86|
    * |          1.0|1.8657142857142857| 2985.442857142857|19.207142857142856|
    * +-------------+------------------+------------------+------------------+
    */
  /**
    * ========================================================================
    * Spark DataFrame and Spark SQL Concepts --Ends
    * ========================================================================
    */


  /**
    * ========================================================================
    * Spark ML  --Begin
    * ========================================================================
    * Extracting Features and Labels to build the Classifier Model
    */

  /**
    * Label     : creditable : 0 or 1
    * Features  : {balance, duration, history, purpose,amount, savings, employment, instPrecentage,sexMarried,
    * guarantors, residenceDuration, assets,age, concCredit, apartment, credits, occupation,dependents, hasPhone, foreign}
    */

  /**
    * Define Feature Vector Columns
    */
  val featureCols = Array("balance", "duration", "history", "purpose", "amount", "savings", "employment", "instPrecentage",
    "sexMarried", "guarantors", "residenceDuration", "assets", "age", "concCredit", "apartment", "credits", "occupation",
    "dependents", "hasPhone", "foreign")

  /**
    * Set the inout and Output Columns to the Vector Assembler
    */
  val assembler: VectorAssembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
  val vectorDF: DataFrame = assembler.transform(creditDF)

  /**
    * Creating a label and Dataframe by transforming the vectorDF
    */
  val labelIndexer = new StringIndexer().setInputCol("creditability").setOutputCol("label")
  val labeledDF = labelIndexer.fit(vectorDF).transform(vectorDF)
  //labelDF.where(labelDF("creditability") ==="0.0").show(5)


  /**
    * Preparing the training and test data
    */


  val splitseed = 5043
  val Array(trainingData, testData) = labeledDF.randomSplit(Array(0.7, 0.3), splitseed)

  /**
    * Train with RandomForrest Classifier
    * maxDepth: Maximum depth of a tree. Increasing the depth makes the model more powerful, but deep trees take longer to train.
    * maxBins:Maximum number of bins used for discretizing continuous features and for choosing how to split on features at each node.
    * impurity:Criterion used for information gain calculation
    * auto:Automatically select the number of features to consider for splits at each tree node.
    * seed :Use a random seed number , allowing to repeat the results
    */
  val classifier = new RandomForestClassifier().setImpurity("gini").setMaxDepth(3).setNumTrees(20)
    .setFeatureSubsetStrategy("auto").setSeed(splitseed)


  val model = classifier.fit(trainingData)


  /**
    * print the random forest trees
    */

  //  println(model.toDebugString)


  /**
    * RandomForestClassificationModel (uid=rfc_a735a421e3fb) with 20 trees
    * Tree 0 (weight 1.0):
    * If (feature 0 <= 1.0)
    * If (feature 10 <= 0.0)
    * If (feature 3 <= 0.0)
    * Predict: 1.0
    * Else (feature 3 > 0.0)
    * Predict: 0.0
    * Else (feature 10 > 0.0)
    * If (feature 12 <= 34.0)
    * Predict: 1.0
    * Else (feature 12 > 34.0)
    * Predict: 0.0
    * Else (feature 0 > 1.0)
    * If (feature 4 <= 2255.0)
    * If (feature 2 <= 3.0)
    * Predict: 0.0
    * Else (feature 2 > 3.0)
    * Predict: 0.0
    * Else (feature 4 > 2255.0)
    * If (feature 15 <= 0.0)
    * Predict: 0.0
    * Else (feature 15 > 0.0)
    * Predict: 0.0
    */


  /**
    * Test the model
    *
    * Running the Model on the test data
    */

  val predictions = model.transform(testData)
  //predictions.show(5)

  /**
    * +-------------+-------+--------+-------+-------+------+-------+----------+--------------+----------+----------+-----------------+------+----+----------+---------+-------+----------+----------+--------+-------+--------------------+-----+--------------------+--------------------+----------+
    * |creditability|balance|duration|history|purpose|amount|savings|employment|instPrecentage|sexMarried|guarantors|residenceDuration|assets| age|concCredit|apartment|credits|occupation|dependents|hasPhone|foreign|            features|label|       rawPrediction|         probability|prediction|
    * +-------------+-------+--------+-------+-------+------+-------+----------+--------------+----------+----------+-----------------+------+----+----------+---------+-------+----------+----------+--------+-------+--------------------+-----+--------------------+--------------------+----------+
    * |          0.0|    0.0|     6.0|    1.0|    6.0|1198.0|    0.0|       4.0|           4.0|       1.0|       0.0|              3.0|   3.0|35.0|       2.0|      2.0|    0.0|       2.0|       0.0|     0.0|    0.0|[0.0,6.0,1.0,6.0,...|  1.0|[12.9404276368705...|[0.64702138184352...|       0.0|
    * |          0.0|    0.0|     6.0|    4.0|    2.0|3384.0|    0.0|       2.0|           1.0|       0.0|       0.0|              3.0|   0.0|44.0|       2.0|      0.0|    0.0|       3.0|       0.0|     1.0|    0.0|(20,[1,2,3,4,6,7,...|  1.0|[14.4063871570449...|[0.72031935785224...|       0.0|
    * |          0.0|    0.0|     9.0|    2.0|    3.0|1366.0|    0.0|       1.0|           3.0|       1.0|       0.0|              3.0|   1.0|22.0|       2.0|      0.0|    0.0|       2.0|       0.0|     0.0|    0.0|(20,[1,2,3,4,6,7,...|  1.0|[10.8490316065663...|[0.54245158032831...|       0.0|
    * |          0.0|    0.0|    12.0|    0.0|    5.0|1108.0|    0.0|       3.0|           4.0|       2.0|       0.0|              2.0|   0.0|28.0|       2.0|      1.0|    1.0|       2.0|       0.0|     0.0|    0.0|(20,[1,3,4,6,7,8,...|  1.0|[13.5136522480229...|[0.67568261240114...|       0.0|
    * |          0.0|    0.0|    12.0|    2.0|    3.0| 727.0|    1.0|       1.0|           4.0|       3.0|       0.0|              2.0|   3.0|33.0|       2.0|      1.0|    0.0|       1.0|       0.0|     1.0|    0.0|[0.0,12.0,2.0,3.0...|  1.0|[12.7445735864865...|[0.63722867932432...|       0.0|
    * +-------------+-------+--------+-------+-------+------+-------+----------+--------------+----------+----------+-----------------+------+----+----------+---------+-------+----------+----------+--------+-------+--------------------+-----+--------------------+--------------------+----------+
    * only showing top 5 rows
    *
    */

  val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
  val accuracy = evaluator.evaluate(predictions)

  println("Accuracy " + accuracy)
  /**
    * Accuracy 0.7178197908286402
    */
}

