package practiceScala

import java.util

class XXX[T](list:util.Iterator[T]) {
  def foreach(f:(T) => Unit): Unit = {
    while (list.hasNext) f(list.next())
  }
}

object implicit_practice {
  def main(args: Array[String]): Unit = {
    val listLinked = new util.LinkedList[Int]()
    listLinked.add(1)
    listLinked.add(2)
    listLinked.add(3)
    val listArray = new util.ArrayList[Int]()
    listArray.add(1)
    listArray.add(2)
    listArray.add(3)

    implicit def aa[T](list:util.LinkedList[T]) = {
      val iter: util.Iterator[T] = list.iterator()
      new XXX(iter)
    }

    implicit def bb[T](list:java.util.ArrayList[T]) ={
      val iter: util.Iterator[T] = list.iterator()
      new XXX(iter)
    }

    listLinked.foreach(println)
    listArray.foreach(println)

    implicit val mm:String = "weww"
    implicit val ssss:Int = 88

    def ooxx(age:Int)(implicit name:String): Unit ={
      println(name + " " + age)
    }

    ooxx(66)
    ooxx(66)("dfd")


  }
}
