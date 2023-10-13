package sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopNTemperature  {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("topN")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data: RDD[String] = sc.parallelize(List(
      "hello world",
      "hello spark",
      "hello world",
      "hello hadoop",
      "hello world",
      "hello msb",
      "hello world",
    ))
    val words: RDD[String] = data.flatMap(_.split(" "))
    val kv: RDD[(String, Int)] = words.map((_, 1))
    val res: RDD[(String, Int)] = kv.reduceByKey(_+_)
    val res01: RDD[(String, Int)] = res.mapValues(x=>x*10)
    val res02: RDD[(String, Iterable[Int])] = res01.groupByKey()
    res02.foreach(println)

    //综合应用算子
    //topN   分组取TopN  （二次排序）
    //2019-6-1	39
    //同月份中 温度最高的2天

    implicit val sdf = new Ordering[(Int, Int)] { // ordering inherited the Comparator interface and ordered extends the Comparable interface
      //implicit usage:
      // implicit view: transform one type value to another, find a compatible method,
      // implicit class:
      // implicit parameters: find out the implicit value in the scope,
      override def compare(x: (Int, Int), y: (Int, Int)): Int = y._2.compareTo(x._2)
    }
    val file: RDD[String] = sc.textFile("src/main/resources/tqdata")
    val data1 = file.map(line => line.split("\t")).map(arr=>{
      //1970-8-8	32

      val arrs: Array[String] = arr(0).split("-")
      (arrs(0).toInt, arrs(1).toInt, arrs(2).toInt, arr(1).toInt)
    })
    //next step

    //first generation:
























  }
}
