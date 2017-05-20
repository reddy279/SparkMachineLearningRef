package demo

/**
  * Created by p5103951 on 5/5/17.
  */
object FunctionalTest {


  def processFile(filename: String, width: Int): Unit = {

    def processLines(line: String): Unit = {

      if (line.length < width) {
        println(line.length + "   --> blahhh....")
      }


    }

    val fileName = scala.io.Source.fromFile(filename)
    for (line <- fileName.getLines()) {
      processLines(line)
    }
  }

  val increase = (x: Int) => x + 1

  val someNumbers = List(-11, -10, -9, 0, 3, 6, 8).filter(x => x>0)


  val newlist = someNumbers.filter(x => x > 0)


}


object FileTest extends App {
  //FunctionalTest.processFile("/Users/p5103951/IdeaProjects/SparkMachineLearningRef/src/main/resources/apctable.csv",100)
  println(FunctionalTest.someNumbers)
}