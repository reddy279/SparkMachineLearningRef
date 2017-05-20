package demo

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by p5103951 on 4/13/17.
  */
object JoinTrick extends App {


  val spark = SparkSession.builder().master("local").appName("JoinTrick").getOrCreate()

  val seq1 = Seq(("Shashi", "07-10-1990", "10"), ("Babu", "08-21-1098", "20"))
  val seq2 = Seq(("Shashi", "1000"), ("Babu", "2000"))


  import spark.sqlContext.implicits._

  val df1: DataFrame = seq1.toDF("name", "DOB", "wages")
  val df2: DataFrame = seq2.toDF("name", "amount")


  val justJoin: DataFrame = df1.join(df2)

  /** This aproach is wrong, this creates duplicate  records .... **/
  /**
    * +------+----------+-----+------+------+
    * |  name|       DOB|wages|  name|amount|
    * +------+----------+-----+------+------+
    * |Shashi|07-10-1990|   10|Shashi|  1000|
    * |Shashi|07-10-1990|   10|  Babu|  2000|
    * |  Babu|08-21-1098|   20|Shashi|  1000|
    * |  Babu|08-21-1098|   20|  Babu|  2000|
    * +------+----------+-----+------+------+
    **/
  val simpleJoin: DataFrame = df1.join(df2, df1.col("name") === df2.col("name"))

  /**
    * +------+----------+-----+------+------+
    * |  name|       DOB|wages|  name|amount|
    * +------+----------+-----+------+------+
    * |Shashi|07-10-1990|   10|Shashi|  1000|
    * |  Babu|08-21-1098|   20|  Babu|  2000|
    * +------+----------+-----+------+------+
    */

   /** Correct One **/

  val coolJoin = df1.join(df2, "name")

  coolJoin.show()
  /**
    * +------+----------+-----+------+
    * |  name|       DOB|wages|amount|
    * +------+----------+-----+------+
    * |Shashi|07-10-1990|   10|  1000|
    * |  Babu|08-21-1098|   20|  2000|
    * +------+----------+-----+------+
    */


}









