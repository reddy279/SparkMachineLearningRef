package demo

/**
  * Created by p5103951 on 5/4/17.
  */




object DefValVar extends App {

//   def  tax(x:Int) = {
//    println("Function execution started.    "+x)
//    1100
//  }



  def  person= new Personal("Shashi", 30)
 println(" Person "+ person.name+ " -- "+person.age)


  person.age=32

  println("second assign :" +person.age)

//  println(tax(2))
//  println(tax(3))
//  println(tax(4))
}
