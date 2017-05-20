package recommandationModel.ml


/**
  * A common task of recommender systems is to improve customer experience
  * through personalized recommendations based on prior user feedback.
  * Collaborative filtering is a technique that is commonly used for recommender systems.
  * Spark ML supports an implementation of matrix factorization for collaborative filtering.
  * Matrix factorization models have consistently shown to perform extremely well for collaborative filtering.
  * The type of matrix factorization we will explore in this example is called explicit matrix factorization
  *
  * =============
  * In explicit matrix factorization --  preferences provided by users themselves are utilized
  * In implicit matrix factorization --  only implicit feedback (e.g. views, clicks, purchases, likes, shares etc.) is utilized
  * =============
  **/

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Movie(movieId: Int, title: String)

case class User(userId: Int, gender: String, age: Int, occupation: Int, zip: String)

case class Rating(userId: Int, movieId: Int, rating: Float)


object MovieRecommander extends App {

  // Parse the movie Input data into Movie Object
  def parseMovie(str: String): Movie = {
    val fields:Array[String] = str.split("::")
    Movie(fields(0).toInt, fields(1))
  }

  def parseUser(str: String): User = {
    val feilds = str.split("::")
    User(feilds(0).toInt, feilds(1), feilds(2).toInt, feilds(3).toInt, feilds(4))
  }

  def parseRating(str: String): Rating = {

    val fields = str.split("::")
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
  }


  val spark = SparkSession.builder().appName("MovieRecommander").master("local").getOrCreate()

  import spark.sqlContext.implicits._

  val movieDF: DataFrame = spark.sparkContext.textFile("/Users/p5103951/IdeaProjects/SparkMachineLearningRef/src/main/resources/movies.dat")
    .map(parseMovie).toDF().na.drop()

  /**
    * movieDF.show(5)
    * +-------+--------------------+
    * |movieId|               title|
    * +-------+--------------------+
    * |      1|    Toy Story (1995)|
    * |      2|      Jumanji (1995)|
    * |      3|Grumpier Old Men ...|
    * |      4|Waiting to Exhale...|
    * |      5|Father of the Bri...|
    * +-------+--------------------+
    */

  val ratingDF: DataFrame = spark.sparkContext.textFile("/Users/p5103951/IdeaProjects/SparkMachineLearningRef/src/main/resources/ratings_smaller.dat")
    .map(parseRating).toDF().na.drop()

  /**
    * ratingDF.show(5)
    * +------+-------+------+
    * |userId|movieId|rating|
    * +------+-------+------+
    * |     1|   1193|   5.0|
    * |     1|    661|   3.0|
    * |     1|    914|   3.0|
    * |     1|   3408|   4.0|
    * |     1|   2355|   5.0|
    * +------+-------+------+
    */

  // Show number of ratings per user.
  val groupdRatings = ratingDF.groupBy("userId").count().withColumnRenamed("count", "No.of.Ratings")

  /**
    * groupdRatings.show(5)
    * +------+-------------+
    * |userId|No.of.Ratings|
    * +------+-------------+
    * |    31|          119|
    * |    85|           39|
    * |    65|          121|
    * |    53|          684|
    * |    78|          140|
    * +------+-------------+
    */

  //Number of User
  // println(" Number of Users " + groupdRatings.count()) //Number of Users 119
  // println(ratingDF.select("userId").distinct().count()) // 119

  // Number of movies in the dataset
  //println(" Number of Movies: " + movieDF.count()) // Number of Movies 3883

  /**
    * Splitting the Rating data into Training(80%) and Test(20%) Datasets
    */
  val Array(training, test) = ratingDF.randomSplit(Array(0.8, 0.2), seed = 0L)

  // Show the resulting dataset counts for training and the test

  val traningDataSetCount = training.count().toDouble / ratingDF.count().toDouble * 100
  val testDataSetCount = test.count().toDouble / ratingDF.count().toDouble * 100

  //  println("Training Data count ::  Test Data Count" + traningDataSetCount + " :: " + testDataSetCount)
  //  println("Total number of ratings = " + ratingDF.count())
  //  println("Training dataset count = " + training.count() + ", " + BigDecimal(traningDataSetCount).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble + "%")
  //  println("Test dataset count = " + test.count() + ", " + BigDecimal(testDataSetCount).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble + "%")

  //Training dataset count = 12017, 80.11%
  //Test dataset count = 2983, 19.89%


  /**
    * Build the Recommendation Model on the Training data using ALS
    *
    */

  val als = new ALS().setMaxIter(10).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
  val model = als.fit(training)


  //println(model.explainParams())

  /**
    * Run the model against the test data
    */

  val predictions = model.transform(test).na.drop()


  /**
    * predictions.show(10)
    * +------+-------+------+----------+
    * |userId|movieId|rating|prediction|
    * +------+-------+------+----------+
    * |    78|    471|   4.0|  2.551445|
    * |     6|   1088|   5.0| 4.8132224|
    * |    38|   1088|   4.0|   3.54067|
    * |     3|   1580|   3.0| 2.5524027|
    * |    17|   1580|   4.0|  4.330088|
    * |     8|   1580|   4.0|  4.284854|
    * |    49|   1580|   3.0| 2.2492254|
    * |    82|   1580|   3.0| 3.5656464|
    * |    25|   1580|   4.0| 4.8915606|
    * |    95|   1580|   4.0| 3.9379427|
    * +------+-------+------+----------+
    */


  /**
    * Evaluate the model by computing the RMSE or MSE ...etc
    * ------------------------------------------------------
    * The Spark ML evaluator for regression, RegressionEvaluator, expects two input columns: prediction and label.
    * RegressionEvaluator supports “rmse” (default), “mse”, “r2”, and “mae”.
    * We will use RMSE, which is the square root of the average of the square of all of the error.
    * RMSE is an excellent general purpose error metric for numerical predictions.
    */

  val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
  val rmse = evaluator.evaluate(predictions)
  //println("Root-mean-square error = " + rmse)
  //Root-mean-square error = 1.6625901515811423

  //This is obviously the case since RMSE is an aggregation of all the error. Thus evaluator.isLargerBetter should be 'false'.

  evaluator.isLargerBetter
  /**
    * Tune the Model
    * --------------
    * Build a Parameter Grid specifying what parameters and values will be
    * evaluated in order to determine the best combination.
    *
    */

  val paramGrid = new ParamGridBuilder().addGrid(als.regParam, Array(0.01, 0.1)).build()
  /**
    * Create a cross validator to tune the model with the defined parameter grid
    * ---------------------------------------------------------------------------
    * Cross-validation attempts to fit the underlying estimator with user-specified
    * combinations of parameters, cross-evaluate the fitted models, and output the best one.
    */

  val cv = new CrossValidator().setEstimator(als).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(2)

  /**
    * Cross-evaluate to find the best model
    * ----------------------------------
    * using the RMSE evaluator and hyperparameters specified in the parameter grid
    *
    */

  val cvModel = cv.fit(training)
  //println("Best fit root-mean-square error = " + evaluator.evaluate(cvModel.transform(test).na.drop()))
  //Best fit root-mean-square error = 1.6625901515811423


  /**
    * Recommend movies to the given User ,Say User 17.
    *
    */

  val userId = 17

  /**
    * Create a DataFrame with the movies the User 17 has rated
    */


  val movies_watched_by_user = ratingDF.filter(ratingDF("userId") === userId)


  /**
    * movies_watched_by_user.show(10)
    * +------+-------+------+
    * |userId|movieId|rating|
    * +------+-------+------+
    * |    17|   1179|   5.0|
    * |    17|   2553|   4.0|
    * |    17|   2554|   3.0|
    * |    17|   3932|   3.0|
    * |    17|   3863|   3.0|
    * |    17|   3793|   4.0|
    * |    17|   1253|   5.0|
    * |    17|    720|   5.0|
    * |    17|   2058|   3.0|
    * |    17|   1185|   5.0|
    * +------+-------+------+
    *
    */

  /**
    * Calculate User 17 minimum, maximum and average movie rating.
    */


  movies_watched_by_user.select(min($"rating"), max($"rating"), avg($"rating")).show()
  /**
    * +-----------+-----------+-----------------+
    * |min(rating)|max(rating)|      avg(rating)|
    * +-----------+-----------+-----------------+
    * |        2.0|        5.0|4.075829383886256|
    * +-----------+-----------+-----------------+
    */

  /**
    * Show User 17's top 10 movies with Title ,Genre, and Rating.
    * Join the rating dataset with Movie Datset for all the data of the User.
    */

  ratingDF.as('a).filter(ratingDF("userId") === userId)
    .join(movieDF.as('b), $"a.movieId" === $"b.movieId")
    .select("a.userId", "a.movieId", "b.title", "a.rating").sort($"a.rating".desc)

  /**
    * .show(10)
    * +------+-------+--------------------+------+
    * |userId|movieId|               title|rating|
    * +------+-------+--------------------+------+
    * |    17|   2662|War of the Worlds...|   5.0|
    * |    17|   2173|Navigator: A Medi...|   5.0|
    * |    17|     34|         Babe (1995)|   5.0|
    * |    17|   3703|Mad Max 2 (a.k.a....|   5.0|
    * |    17|   1653|      Gattaca (1997)|   5.0|
    * |    17|   1270|Back to the Futur...|   5.0|
    * |    17|   1265|Groundhog Day (1993)|   5.0|
    * |    17|   3175| Galaxy Quest (1999)|   5.0|
    * |    17|    296| Pulp Fiction (1994)|   5.0|
    * |    17|    593|Silence of the La...|   5.0|
    * +------+-------+--------------------+------+
    */

  /**
    * Determine what movies that User 17 has not watched and rated , so that we can recommend a new movie.
    * This is an Inner Join where it provides all the ratings in "Rating" Dataset not rated by user 17.
    */


  val movie_user_not_watched = ratingDF.filter(ratingDF("userId") !== userId)
  // println(movies_watched_by_user.count()) // 211

  /**
    * movie_user_not_watched.show(5)
    * +------+-------+------+
    * |userId|movieId|rating|
    * +------+-------+------+
    * |     1|   1193|   5.0|
    * |     1|    661|   3.0|
    * |     1|    914|   3.0|
    * |     1|   3408|   4.0|
    * |     1|   2355|   5.0|
    * +------+-------+------+
    */


  /**
    * Determine what movies that User 17 has not watched and rated , so that we can recommend a new movie
    * Ratings , Movie --> Apply right outer join
    *
    * This Results -> List all the movies from the "Movies" Dataset that User 17 has not Rated.We don't want to recommend that user has
    * already rated.
    *
    */

  val movies_user_not_watched_movieId = ratingDF.filter(ratingDF("userId") === userId).as('a)
    .join(movieDF.as('b), $"a.movieId" === $"b.movieId", "right")
    .filter($"a.rating".isNull).select($"b.movieId", $"b.title").sort($"b.movieId".asc)

  /**
    * movies_user_not_watched_movieId.show(5)
    * +-------+--------------------+
    * |movieId|               title|
    * +-------+--------------------+
    * |      1|    Toy Story (1995)|
    * |      2|      Jumanji (1995)|
    * |      3|Grumpier Old Men ...|
    * |      4|Waiting to Exhale...|
    * |      5|Father of the Bri...|
    * +-------+--------------------+
    */


  /**
    * verification on the Count
    *
    * Now the number movies not watched by the user from the movies dataset plus the movies not rated should be
    * equal to total number of movies in the Movies dataset.
    *
    */

  //  println("Total Number of Movies in Movies Dataset" + movieDF.count)
  //  println(" Total Number of movies rated by User:  " + ratingDF.filter(ratingDF("userId") === userId).count())
  //  println("Total Number of Movies not rated by User " + movies_user_not_watched_movieId.count())

  //Total Number of Movies in Movies Dataset3883
  // Total Number of movies rated by User:  211
  //Total Number of Movies not rated by User 3672


  /**
    * ALS alogorithm requires two input columns UserId and MovieId
    * Create a new DataFrame
    */


  val added_UserId = movies_user_not_watched_movieId.withColumn("userId", lit(userId))
  /**
    * added_UserId.show(5)
    * +-------+--------------------+------+
    * |movieId|               title|userId|
    * +-------+--------------------+------+
    * |      1|    Toy Story (1995)|    17|
    * |      2|      Jumanji (1995)|    17|
    * |      3|Grumpier Old Men ...|    17|
    * |      4|Waiting to Exhale...|    17|
    * |      5|Father of the Bri...|    17|
    * +-------+--------------------+------+
    */


  /**
    * Now Final Recommendation for the user 17
    *
    */


  val prediction_for_user = cvModel.transform(added_UserId).na.drop()
  prediction_for_user.select("title", "prediction").sort($"prediction".desc)

  /**
    * .show(10)
    * +--------------------+----------+
    * |               title|prediction|
    * +--------------------+----------+
    * |Joy Luck Club, Th...|   6.49559|
    * | Animal House (1978)| 5.8364043|
    * |Monty Python's Li...|  5.787142|
    * |Harold and Maude ...|  5.758556|
    * |   Sting, The (1973)| 5.7401004|
    * |Raiders of the Lo...| 5.6815615|
    * |Monty Python and ...| 5.5674863|
    * |       Grease (1978)| 5.4330406|
    * |13th Warrior, The...| 5.4044857|
    * |  Three Kings (1999)| 5.3805895|
    * +--------------------+----------+
    */
}
















