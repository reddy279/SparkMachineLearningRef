package demo



import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by p5103951 on 4/13/17.
  */
object DedupKeepLatest extends App {


  val spark = SparkSession.builder().master("local").appName("DedupKeepLatest").getOrCreate()

  import spark.sqlContext.implicits._


  val custs = Seq(
    (1, "Widget Co", 120000.00, 0.00, "AZ"),
    (2, "Acme Widgets", 410500.00, 500.00, "CA"),
    (3, "Widgetry", 410500.00, 200.00, "PA"),
    (4, "Widgets R Us", 410500.00, 0.0, "CA"),
    (3, "Widgetry", 410500.00, 200.00, "CA"),
    (5, "Ye Olde Widgete", 500.00, 0.0, "MA"),
    (6, "Widget Co", 12000.00, 10.00, "AZ")
  ).toDF("id", "name", "sales", "discount", "state")



  custs.dropDuplicates("name","state").show()



}
