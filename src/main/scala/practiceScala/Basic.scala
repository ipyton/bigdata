package practiceScala

package practiceScala

import scala.collection.immutable
import java.util
import java.util.Date

object ooxx {
  //静态单例类，不需要被初始化，main必须写入object中。
  println("12") //在类加载的时候就初始化了
  private val xo = new ooxx(11) // 与class 共享private
  def main(args: Array[String]): Unit = { // scala的性能较慢
    var a = 10
    if (a <= 0) {

    } else {

    }

    var aa = 0
    while (aa <= 20) {
      aa += 10
      println(aa)
    }

    println("hello world")
    //println(xo.age)

    val seqs = 1 until 10
    println(seqs)

    for (i <- seqs if (i % 2 == 0)) {
      println(i)
    }

    var num = 0

    for (i <- 1 to 9; j <- 1 to 9; k <- 1 to 9 if (j < i && i < k)) {
      println(s"${i} + ${j}")
    }

    val seqss: immutable.IndexedSeq[Int] = for (i <- 1 to 10) yield {
      var x = 8
      i + x
    }

    for (i <- seqss) {
      println(i)
    }

    def func01(): Unit = {
      println("hello world")
    }

    var y: Unit = func01()
    println(y)

    def func02(): Unit = {
      new util.LinkedList[String]()
    }

    def func03(a: Int): Unit = {
      println(a)
    }

    func03(20)

    def func04(from: Int, to: Int): Int = {
      if (from >= to) {
        return from
      }
      from + func04(from + 1, to)
    }

    println(func04(1, 100))

    def func05(a: Int = 8, b: String = "abc"): Unit = {
      println(s"$a\t$b")
    }

    func05(b = "ooxx")

    var xx: Int = 3

    //anonymous function

    var yy: (Int, Int) => Int = (a: Int, b: Int) => {
      a + b
    }
    val w: Int = yy(3, 4)
    print(w)

    //嵌套函数
    def func06(a: String): Unit = {
      def func05(): Unit = {
        print(a)
      }

      func05()
    }

    func06("hello")

    //部分调用
    def func7(date: Date, tp: String, msg: String): Unit = {
      println(s"$date\t$tp\t$msg")
    }

    func7(new Date(), "info", "ok")

    var info = func7(_: Date, "info", _: String)
    var error = func7(_: Date, "Error", _: String)
    info(new Date, "ok")
    error(new Date, "error...")

    //可变参数

    def func08(a: Int*): Unit = {
      for (e <- a) {
        println(e)
      }
      a.foreach((x : Int) => {println(x)})
      a.foreach(println)
    }

    func08(20)
    func08(1, 2, 3, 4, 5, 6, 7, 8, 9)

    //高阶函数:函数作为参数，函数作为返回值
    def computer(a: Int, b: Int, f: (Int, Int) => Int): Unit = {
      val res: Int = f(a, b)
      println(res)
    }
    computer(3, 8, (x: Int, y: Int) => {
      x * y
    })

    computer(9, 9, _ * _)


    def factory(i: String): (Int, Int) => Int = {
      def plus(x: Int, y: Int): Int = {
        x + y
      }
      def subtract(x: Int, y: Int): Int = {
        x - y
      }
      if (i.equals("+")) {
        plus
      }
      else if (i.equals("-")){
        subtract
      }
      else {
        (x: Int, y: Int) => {
          x * y
        }
      }
    }

    computer(3, 8, factory("-"))


    println("currying")
    def func09(a: Int)(b: Int)(c: String): Unit = {
      println(s"$a $b $c")
    }

    val c = func09(10) _
    c(20)("sds")

    func09(3)(8)("sdfdsdf")

    def func10(a: Int*)(b: String*): Unit = {
      a.foreach(println)
      b.foreach(println)
    }

    func10(1,2,3)("sdss", "sss")
    val ccd = func10 _
    ccd(List(1,2,3))(List("sdss", "sss")) // 只有这样才可以正常调用

  }
}


class ooxx(sex: String) {
  //主构造函数
  println("ooxx1")
  var name = "class:ooxx"
  var age = 0
  println("ooxx2")
  def this(age: Int) {
    this("abd")//第一行必须调用主构造函数
    this.age = age
  }

  def printMsg(): Unit = {//不返回任何值
    println(s"sex: ${name}")
    println()
    println(s"ooxx${age}")
  }


}
