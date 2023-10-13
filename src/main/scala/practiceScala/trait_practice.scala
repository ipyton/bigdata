package practiceScala


//trait 是做什么的
trait God{
  def say(): Unit ={
    println("god ...say")
  }
}
trait Devil{
  def destroy(): Unit ={
    println("devil say")
  }
  def ruin(): Unit
}


class Person(name: String) extends God with Devil { //
  override def ruin(): Unit = {
    println("I am ruin")
  }

  def hello(): Unit={
    println("say hello")
  }

}

object trait_practice {
  def xxx:PartialFunction[Any, String] = { //接受类型为any的参数,返回类型为String的参数
    case "hello" => "val is hello"
    case x:Int => s"$x .. is int"
    case _ => "done"
  }

  def main(args: Array[String]): Unit = {
    val p = new Person("zhangsan")
    p.hello()
    p.ruin()
    p.destroy()
    p.say()

    val p2 = new Person("zhangsan")
    println(p == p2)
    println(p.equals(p2))

    val  tup: (Double, Int, String, Boolean, Int) = (1.0, 8, "abc", false, 80)

    val iter: Iterator[Any] = tup.productIterator
    val res: Iterator[Unit] = iter.map(
      (x) => {
        x match {
          case 1 => println(s"$x .. is 1")
          case 8 => println(s"$x .. is 8")
          case false => println(s"$x .. is false")
          case w: Int if w > 50 => println(s"$w is > 50")
          case _ => println(s"$x unknown type")
        }
      }
    )
    while (res.hasNext) println(res.next())
    println(xxx(44))
    println(xxx("hello"))
    println(xxx("hi"))


  }

}


