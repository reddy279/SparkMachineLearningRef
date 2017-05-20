package demo

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

/**
  * Created by p5103951 on 4/10/17.
  */

case class Salary(depName: String, empNo: Long, salary: Long)
case class Person(name: String, age: Int)


object WindowFunction extends App {

  val spark = SparkSession.builder().master("local").appName("WindowFunction").getOrCreate()

  import spark.sqlContext.implicits._

  val empSalary = Seq(
    Salary("sales", 1, 5000),
    Salary("personnel", 2, 3900),
    Salary("sales", 3, 4800),
    Salary("sales", 4, 4800),
    Salary("personnel", 5, 3500),
    Salary("develop", 7, 4200),
    Salary("develop", 8, 6000),
    Salary("develop", 9, 4500),
    Salary("develop", 10, 5200),
    Salary("develop", 11, 5200)
  ).toDF()

  val people = Seq(Person("Jacek", 42), Person("Patryk", 19), Person("Maksym", 5)).toDF().show()

  /**
    * +---------+-----+------+
    * |  depName|empNo|salary|
    * +---------+-----+------+
    * |    sales|    1|  5000|
    * |personnel|    2|  3900|
    * |    sales|    3|  4800|
    * |    sales|    4|  4800|
    * |personnel|    5|  3500|
    * |  develop|    7|  4200|
    * |  develop|    8|  6000|
    * |  develop|    9|  4500|
    * |  develop|   10|  5200|
    * |  develop|   11|  5200|
    * +---------+-----+------+
    */

  val depByName: WindowSpec = Window.partitionBy('depName)



  empSalary.withColumn("avg", avg('salary) over depByName).show()

  /**
    * +---------+-----+------+-----------------+
    * |  depName|empNo|salary|              avg|
    * +---------+-----+------+-----------------+
    * |  develop|    7|  4200|           5020.0|
    * |  develop|    8|  6000|           5020.0|
    * |  develop|    9|  4500|           5020.0|
    * |  develop|   10|  5200|           5020.0|
    * |  develop|   11|  5200|           5020.0|
    * |    sales|    1|  5000|4866.666666666667|
    * |    sales|    3|  4800|4866.666666666667|
    * |    sales|    4|  4800|4866.666666666667|
    * |personnel|    2|  3900|           3700.0|
    * |personnel|    5|  3500|           3700.0|
    * +---------+-----+------+-----------------+
    */


}
