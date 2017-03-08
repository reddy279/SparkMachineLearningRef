package classification.logisticRegression.ml

import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by p5103951 on 2/2/17.
  */
object PredictCancer extends App {


  /**
    *   1. Sample code number:           id number
    *   2. Clump Thickness:              1 - 10
    *   3. Uniformity of Cell Size:      1 - 10
    *   4. Uniformity of Cell Shape:     1 - 10
    *   5. Marginal Adhesion:            1 - 10
    *   6. Single Epithelial Cell Size:  1 - 10
    *   7. Bare Nuclei:                  1 - 10
    *   8. Bland Chromatin:              1 - 10
    *   9. Normal Nucleoli:              1 - 10
    *   10. Mitoses:                     1 - 10
    *   11. Class:                       (2 for benign, 4 for malignant)
    */

  /**
    *
    * Case Class to Define Cancer Dataset Schema.
    */
  case class Observation(clas: Double, thickness: Double, size: Double, shape: Double, madh: Double, epsize: Double,
                         bnuc: Double, bchrom: Double, nNuc: Double, mito: Double)


  /**
    * Function to Create Observation class from an Array of Double.
    * Class Malignant is changed to 1 for labeling.
    *
    * Lable = Malignant = 1  , Benign =0
    * Features = {thickness, size, shape, madh, epsize,bnuc, bchrom, nNuc, mito}
    *
    */

  def parseObservation(line: Array[Double]): Observation = {

    Observation(
      if (line(9) == 4.0) 1 else 0, line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8)
    )
  }

  /**
    * Function to transform an RDD of Strings to RDD of Array of Double and filter the lines with "?" ,
    * remove the first column.
    */

  def pasrseRDD(rdd: RDD[String]): RDD[Array[Double]] = {

    rdd.map(_.split(",")).filter(_ (6) != "?").map(_.drop(1)).map(_.map(_.toDouble))
  }


  val spark = SparkSession.builder().appName("PredictCancer").master("local").getOrCreate()

  val file_rdd: RDD[String] = spark.sparkContext.textFile("/Users/p5103951/IdeaProjects/SparkMachineLearningRef/src/main/resources/Cancer_Data.csv")

  /**
    * Transform String based data to Double and map to RDD of Observation Object
    */
  val observationRdd: RDD[Observation] = pasrseRDD(file_rdd).map(parseObservation)


  /**
    * importing implicits to convert the RDD to DF
    */

  import spark.sqlContext.implicits._


  /**
    * Convert the RDD to DF and register a table.
    */
  val observationDF: DataFrame = observationRdd.toDF().cache()

  //observationDF.show(10)
  // observationDF.registerTempTable("observation")

  /** +----+---------+----+-----+----+------+----+------+----+----+
    * |clas|thickness|size|shape|madh|epsize|bnuc|bchrom|nNuc|mito|
    * +----+---------+----+-----+----+------+----+------+----+----+
    * | 0.0|      5.0| 1.0|  1.0| 1.0|   2.0| 1.0|   3.0| 1.0| 1.0|
    * | 0.0|      5.0| 4.0|  4.0| 5.0|   7.0|10.0|   3.0| 2.0| 1.0|
    * | 0.0|      3.0| 1.0|  1.0| 1.0|   2.0| 2.0|   3.0| 1.0| 1.0|
    * | 0.0|      6.0| 8.0|  8.0| 1.0|   3.0| 4.0|   3.0| 7.0| 1.0|
    * | 0.0|      4.0| 1.0|  1.0| 3.0|   2.0| 1.0|   3.0| 1.0| 1.0|
    * | 1.0|      8.0|10.0| 10.0| 8.0|   7.0|10.0|   9.0| 7.0| 1.0|
    * | 0.0|      1.0| 1.0|  1.0| 1.0|   2.0|10.0|   3.0| 1.0| 1.0|
    * | 0.0|      2.0| 1.0|  2.0| 1.0|   2.0| 1.0|   3.0| 1.0| 1.0|
    * | 0.0|      2.0| 1.0|  1.0| 1.0|   2.0| 1.0|   1.0| 1.0| 5.0|
    * | 0.0|      4.0| 2.0|  1.0| 1.0|   2.0| 1.0|   2.0| 1.0| 1.0|
    * +----+---------+----+-----+----+------+----+------+----+----+
    * */

  /**
    * Define Feature columns to put in the feature Vector.
    */
  val featureCols = Array("thickness", "size", "shape", "madh", "epsize", "bnuc", "bchrom", "nNuc", "mito")

  /**
    * Set the Input and Output columns.
    */
  val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")


  /**
    * Return a DataFrame with all the feature Columns in Vectore Column
    */
  val assemblerDF: DataFrame = assembler.transform(observationDF)

  //assemberDF.show(10)

  /**
    * +----+---------+----+-----+----+------+----+------+----+----+--------------------+
    * |clas|thickness|size|shape|madh|epsize|bnuc|bchrom|nNuc|mito|            features|
    * +----+---------+----+-----+----+------+----+------+----+----+--------------------+
    * | 0.0|      5.0| 1.0|  1.0| 1.0|   2.0| 1.0|   3.0| 1.0| 1.0|[5.0,1.0,1.0,1.0,...|
    * | 0.0|      5.0| 4.0|  4.0| 5.0|   7.0|10.0|   3.0| 2.0| 1.0|[5.0,4.0,4.0,5.0,...|
    * | 0.0|      3.0| 1.0|  1.0| 1.0|   2.0| 2.0|   3.0| 1.0| 1.0|[3.0,1.0,1.0,1.0,...|
    * | 0.0|      6.0| 8.0|  8.0| 1.0|   3.0| 4.0|   3.0| 7.0| 1.0|[6.0,8.0,8.0,1.0,...|
    * | 0.0|      4.0| 1.0|  1.0| 3.0|   2.0| 1.0|   3.0| 1.0| 1.0|[4.0,1.0,1.0,3.0,...|
    * | 1.0|      8.0|10.0| 10.0| 8.0|   7.0|10.0|   9.0| 7.0| 1.0|[8.0,10.0,10.0,8....|
    * | 0.0|      1.0| 1.0|  1.0| 1.0|   2.0|10.0|   3.0| 1.0| 1.0|[1.0,1.0,1.0,1.0,...|
    * | 0.0|      2.0| 1.0|  2.0| 1.0|   2.0| 1.0|   3.0| 1.0| 1.0|[2.0,1.0,2.0,1.0,...|
    * | 0.0|      2.0| 1.0|  1.0| 1.0|   2.0| 1.0|   1.0| 1.0| 5.0|[2.0,1.0,1.0,1.0,...|
    * | 0.0|      4.0| 2.0|  1.0| 1.0|   2.0| 1.0|   2.0| 1.0| 1.0|[4.0,2.0,1.0,1.0,...|
    * +----+---------+----+-----+----+------+----+------+----+----+--------------------+
    * only showing top 10 rows
    */


  /**
    * Use StringIndexer to return a DataFrame with "clas" (Malignant /Benign) column added as a "lable"
    **/

  val lableIndexer: StringIndexer = new StringIndexer().setInputCol("clas").setOutputCol("label")
  val lableIndexerDF: DataFrame = lableIndexer.fit(assemblerDF).transform(assemblerDF)

  //lableIndexerDF.show(5)
  /**
    * +----+---------+----+-----+----+------+----+------+----+----+--------------------+-----+
    * |clas|thickness|size|shape|madh|epsize|bnuc|bchrom|nNuc|mito|            features|label|
    * +----+---------+----+-----+----+------+----+------+----+----+--------------------+-----+
    * | 0.0|      5.0| 1.0|  1.0| 1.0|   2.0| 1.0|   3.0| 1.0| 1.0|[5.0,1.0,1.0,1.0,...|  0.0|
    * | 0.0|      5.0| 4.0|  4.0| 5.0|   7.0|10.0|   3.0| 2.0| 1.0|[5.0,4.0,4.0,5.0,...|  0.0|
    * | 0.0|      3.0| 1.0|  1.0| 1.0|   2.0| 2.0|   3.0| 1.0| 1.0|[3.0,1.0,1.0,1.0,...|  0.0|
    * | 0.0|      6.0| 8.0|  8.0| 1.0|   3.0| 4.0|   3.0| 7.0| 1.0|[6.0,8.0,8.0,1.0,...|  0.0|
    * | 0.0|      4.0| 1.0|  1.0| 3.0|   2.0| 1.0|   3.0| 1.0| 1.0|[4.0,1.0,1.0,3.0,...|  0.0|
    * +----+---------+----+-----+----+------+----+------+----+----+--------------------+-----+
    * only showing top 5 rows
    *
    */


  /**
    * Split the DataFramw into Training and Test Data with the ratio of 70%  and 30% respectively
    *
    */

  val splitSeed = 5043
  val Array(trainingData, testData) = lableIndexerDF.randomSplit(Array(0.7, 0.3), splitSeed)

  /**
    * ====================================================================
    * TRAIN THE MODEL
    * ====================================================================
    */


  /**
    * Create the Classifier , set parameter for training
    */
  val logisticRegression = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

  /**
    * Use the logistic regression to train (fit) the model wit the training data.
    */

  val model: LogisticRegressionModel = logisticRegression.fit(trainingData)


  //println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

  //Coefficients: (9,[1,2,5,6],[0.06036524114798738,0.06349108906034746,0.08330069924780688,8.280292114686343E-5]) Intercept: -1.4339584751086758


  /**
    * ====================================================================
    * TEST THE MODEL
    * ====================================================================
    */

  /**
    * run the model on the Test Features to get the prediction
    */
  val predictions: DataFrame = model.transform(testData)


  // predictions.select("clas","label", "prediction").show(5)
  // predictions.show()
  /**
    * +----+---------+----+-----+----+------+----+------+----+----+--------------------+-----+--------------------+--------------------+----------+
    * |clas|thickness|size|shape|madh|epsize|bnuc|bchrom|nNuc|mito|            features|label|       rawPrediction|         probability|prediction|
    * +----+---------+----+-----+----+------+----+------+----+----+--------------------+-----+--------------------+--------------------+----------+
    * | 0.0|      1.0| 1.0|  1.0| 1.0|   1.0| 1.0|   1.0| 1.0| 1.0|[1.0,1.0,1.0,1.0,...|  0.0|[1.22671864273138...|[0.77324374403458...|       0.0|
    * | 0.0|      1.0| 1.0|  1.0| 1.0|   1.0| 1.0|   1.0| 1.0| 1.0|[1.0,1.0,1.0,1.0,...|  0.0|[1.22671864273138...|[0.77324374403458...|       0.0|
    * | 0.0|      1.0| 1.0|  1.0| 1.0|   1.0| 1.0|   2.0| 1.0| 1.0|[1.0,1.0,1.0,1.0,...|  0.0|[1.22663583981024...|[0.77322922521940...|       0.0|
    * | 0.0|      1.0| 1.0|  1.0| 1.0|   1.0| 1.0|   2.0| 1.0| 1.0|[1.0,1.0,1.0,1.0,...|  0.0|[1.22663583981024...|[0.77322922521940...|       0.0|
    * | 0.0|      1.0| 1.0|  1.0| 1.0|   2.0| 1.0|   1.0| 1.0| 1.0|[1.0,1.0,1.0,1.0,...|  0.0|[1.22671864273138...|[0.77324374403458...|       0.0|
    * | 0.0|      1.0| 1.0|  1.0| 1.0|   2.0| 1.0|   1.0| 1.0| 1.0|[1.0,1.0,1.0,1.0,...|  0.0|[1.22671864273138...|[0.77324374403458...|       0.0|
    * | 0.0|      1.0| 1.0|  1.0| 1.0|   2.0| 1.0|   2.0| 1.0| 1.0|[1.0,1.0,1.0,1.0,...|  0.0|[1.22663583981024...|[0.77322922521940...|       0.0|
    * | 0.0|      1.0| 1.0|  1.0| 1.0|   2.0| 1.0|   2.0| 1.0| 1.0|[1.0,1.0,1.0,1.0,...|  0.0|[1.22663583981024...|[0.77322922521940...|       0.0|
    * +----+---------+----+-----+----+------+----+------+----+----+--------------------+-----+--------------------+--------------------+----------+
    *
    */

  // predictions.filter($"prediction" > 0.0).show()
  //predictions.where(predictions("prediction") > 0.0).show(5)


  val trainingSummary = model.summary
  val objectiveHistory = trainingSummary.objectiveHistory

  objectiveHistory.foreach(loss => println(loss))

  val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

  /**
    * Receiver Operating Characterstic as a DataFrame
    */

  val roc: DataFrame = binarySummary.roc

  roc.show(10)
  /**
    * Area under ROC
    */

  val aRoc: Double = binarySummary.areaUnderROC


  /**
    * Set the Model threshold to maximize F-Measure
    */

  val fMeasure: DataFrame = binarySummary.fMeasureByThreshold
  val fm = fMeasure.col("F-Measure")

  val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)

  val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure).select("threshold").head().getDouble(0)
  model.setThreshold(bestThreshold)

  val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")

  val accuracy: Double = evaluator.evaluate(predictions)
  println("Accuracy : " + accuracy)


}
