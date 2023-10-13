package sparkcore

import breeze.numerics.constants.Database.list
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object RDDAPIs {
  def main(args :Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test1");
    val sc = new SparkContext(conf);
    val dataRDD: RDD[Int] = sc.parallelize(List(1,2,3,4,5,4,3,2,1)) //list: immutable create a RDD
    val filterRDD: RDD[Int] = dataRDD.filter(_> 3)
    val res: Array[Int] = filterRDD.collect() //转化为数组 mutable
    res.foreach(println)

    println("------")

    val res1: RDD[Int] = dataRDD.map((_,1)).reduceByKey(_ + _).map(_._1)
    res1.foreach(println)

    val resx : RDD[Int] = dataRDD.distinct()
    resx.foreach(println)


    val rdd1: RDD[Int] = sc.parallelize( List( 1,2,3,4,5)  )
    val rdd2: RDD[Int] = sc.parallelize( List( 3,4,5,6,7)  )

    rdd1.subtract(rdd2)
    rdd1.intersection(rdd2)

    val value = rdd1.cartesian(rdd2)

    val unitRDD: RDD[Int] = rdd1.union(rdd2)

    val kv1: RDD[(String, Int)] = sc.parallelize(List(
      ("zhangsan", 11),
      ("zhangsan", 12),
      ("lisi", 13),
      ("wangwu", 14)
    ))
    val kv2: RDD[(String, Int)] = sc.parallelize(List(
      ("zhangsan", 21),
      ("zhangsan", 22),
      ("lisi", 23),
      ("zhaoliu", 28)
    ))

    val cogroup: RDD[(String, (Iterable[Int], Iterable[Int]))] = kv1.cogroup(kv2)

    cogroup.foreach(println) // accumulate values of same keys

    //PV,UV
    //需求：根据数据计算各网站的PV,UV，同时，只显示top5
    //解题：要按PV值，或者UV值排序，取前5名


    val file: RDD[String] = sc.textFile("src/main/resources/spark_data/pvuvdata", 5) // tasks
    //sample of data: 42.62.88.214	新疆	2018-11-12	1542011088714	734986595720971991	www.baidu.com	Click
    val pair: RDD[(String, Int)] = file.map(line=> (line.split("\t")(5), 1))

    val reduce: RDD[(String, Int)] = pair.reduceByKey(_+_)
    val map:RDD[(Int, String)] = reduce.map(_.swap) //swap the key and value
    val sorted: RDD[(Int, String)] = map.sortByKey(false)
    val resl: RDD[(String, Int)] = sorted.map(_.swap)
    val pv: Array[(String, Int)] = resl.take(5)
    pv.foreach(println)


    val keys: RDD[(String, String)] = file.map(
      line => {
        val strs: Array[String] = line.split("\t")
        (strs(5), strs(0))
      }
    )
    val key: RDD[(String, String)] = key.distinct()
    val pairx: RDD[(String, Int)] = key.map(k => (k._1, 1))
    val uvreduce: RDD[(String, Int)] = pairx.reduceByKey(_ + _)
    val uvSorted: RDD[(String, Int)] = uvreduce.sortBy(_._2, false)
    val uv: Array[(String, Int)] = uvSorted.take(5)

    uv.foreach(println)

    val data: RDD[(String, Int)] = sc.parallelize(List(
      ("zhangsan", 234),
      ("zhangsan", 5667),
      ("zhangsan", 343),
      ("lisi", 212),
      ("lisi", 44),
      ("lisi", 33),
      ("wangwu", 535),
      ("wangwu", 22)
    ))

    val group: RDD[(String, Iterable[Int])] = data.groupByKey()
    group.foreach(println)

    val res01: RDD[(String, Int)] = group.flatMap(e => e._2.map(x => (e._1, x )).iterator)
    res01.foreach(println)

    println("-------------------------")
    group.flatMapValues(e => e.iterator).foreach(println)

    println("-------------------------")
    group.mapValues(e => e.toList.sorted.take(2)).foreach(println)

    println("-------------------------")
    group.flatMapValues(e => e.toList.sorted.take(2)).foreach(println)

    println("--------------sum,count,min,max,avg")
    val sum: RDD[(String, Int)] = data.reduceByKey(_ + _)
    val max: RDD[(String, Int)] = data.reduceByKey((ov, nv)=> if (ov > nv) ov else nv)
    val min: RDD[(String, Int)] = data.reduceByKey((ov, nv)=> if (ov < nv) ov else nv)
    val count:RDD[(String, Int)] = data.mapValues(e=>1).reduceByKey(_+_)
    val tmp: RDD[(String, (Int, Int))] = sum.join(count)
    val avg:RDD[(String, Int)] = tmp.mapValues(e => e._1/e._2)
    println("________sum _______________")
    sum.foreach(println)
    println("_________max_______________")
    max.foreach(println)
    println("___________min_____________")
    min.foreach(println)
    println("-----count-----------------")
    count.foreach(println)
    avg.foreach(println)

    val tmpx:RDD[(String, (Int,Int))] = data.combineByKey(
      (value: Int) => (value, 1),// the way of the first value put into the state
      (oldValue:(Int, Int), newValue:Int) => (oldValue._1 +newValue, oldValue._2 + 1), //the second and following how to be put into the hashmap
      (v1: (Int, Int), v2:(Int, Int)) => (v1._1 + v2._1, v1._2 + v2._2) // how to merge
    )
    tmpx.mapValues(e=> e._1/e._2).foreach(println)


    // high level usage
    val data2: RDD[(Int,Int)] = sc.parallelize(1 to 10, 5)
    data2.foreach(println)
    println(s"Data:${data2.getNumPartitions}")



    val repartition: RDD[(Int, Int)] = data2.coalesce(3, false) //key,value


    val data3: RDD[(Int, (Int, Int))] = repartition.mapPartitionsWithIndex(
      (index, pt) => { // pt an RDD which is in the same partition
        pt.map(e => (index, e)) // (index, (key, values))
      }
    )

    println(s"${data3.getNumPartitions}")

    //sql query external join
    val data8: RDD[Int] = sc.parallelize(1 to 10,2)

    val res02: RDD[String] = data8.map(

      (value: Int) => {
        println("------conn--mysql----")
        println(s"-----select $value-----")
        println("-----close--mysql------")
        value + "selected"
      }
    )
    res02.foreach(println)

    //the risk of memory overflow
    val res3: RDD[String] = data8.mapPartitionsWithIndex(
      (pindex, piter) => {
        val lb = new ListBuffer[String]  //致命的！！！！  根据之前源码发现  spark就是一个pipeline，迭代器嵌套的模式
        //数据不会再内存积压
        println(s"--$pindex----conn--mysql----")
        while (piter.hasNext) {
          val value: Int = piter.next()
          println(s"---$pindex--select $value-----")
          lb.+=(value + "selected")
        }
        println("-----close--mysql------")
        lb.iterator
      }
    )
    res3.foreach(println)

    // iterator embed with another iterator
    val res4: RDD[String] = data8.mapPartitionsWithIndex(
      (pindex, piter) => {

        new Iterator[String] {
          println(s"---$pindex--conn--mysql------")

          override def hasNext = if (piter.hasNext == false) {
            println(s"---$pindex---close--mysql"); false
          } else true

          override def next() = {
            val value: Int = piter.next()
            println(s"---$pindex--select $value-----")
            value + "selected"
          }
        }
      }
    )
    res4.foreach(println)

    tuning_about_reuse()
  }

  def tuning_about_reuse(): Unit ={
    val conf: SparkConf = new SparkConf().setAppName("persistant").setMaster("local")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    sc.setCheckpointDir("./src/main/resources")
    val data: RDD[Int] = sc.parallelize(1 to 10)

    val d2rdd: RDD[(String, Int)] = data.map(e=> if(e%2 == 0) ("A", e) else ("B",e))

    d2rdd.cache() //this method uses persist(MEMORY_ONLY)
    //d2rdd.persist(StorageLevel.MEMORY_AND_DISK)
    //d2rdd.persist(StorageLevel.MEMORY_ONLY_SER)
    d2rdd.checkpoint() // check point will calculate the whole chain again. so it is better to cache first and cache(checkpoint will take the results from the cache directly and avoid calculating again)


    val group: RDD[(String, Iterable[Int])] = d2rdd.groupByKey()
    group.foreach(println)   //第一个作业  job01

    //奇偶统计：
    val kv1: RDD[(String, Int)] = d2rdd.mapValues(e=> 1)
    val reduce: RDD[(String, Int)] = kv1.reduceByKey(_+_)
    reduce.foreach(println)   //第三个作业  job03

    val res: RDD[(String, String)] = reduce.mapValues(e=> e+" chen")
    res.foreach(println)    //第四个作业  job04
  }

  def other_features(): Unit ={
    val conf: SparkConf = new SparkConf().setAppName("contrl").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")



    val data: RDD[Int] = sc.parallelize(1 to 10,2)


    var n = 0
    val ox: LongAccumulator = sc.longAccumulator("ooxx")


    val count: Long = data.map(x => {

      if(x%2==0) ox.add(1)  else ox.add(100)
      n+=1
      println(s"executor:n: $n")
      x
    }).count()
    // regular variables can not be push to driver when executor finish its tasks. so an accumulator is necessary.
    println(s"count:  $count")
    println(s"Driver:n: $n")
    println(s"Driver:ox:${ox}")
    println(s"Driver:ox.avg:${ox.avg}")
    println(s"Driver:ox.sum:${ox.sum}")
    println(s"Driver:ox.count:${ox.count}")


    val data1: RDD[String] = sc.parallelize(List(
      "hello world",
      "hello spark",
      "hi world",
      "hello msb",
      "hello world",
      "hello hadoop"
    ))
    val list: Array[String] = data1.map((_,1)).reduceByKey(_+_).sortBy(_._2,false).keys.take(2)
    // code is push to the excutor and it results is returned to the driver


    val blist: Broadcast[Array[String]] = sc.broadcast(list)  //第一次见到broadcast是什么时候  taskbinary



    val res: RDD[String] = data1.filter(    x=>  blist.value.contains(x)     )   //external variables want to be used by the internal tasks, it must be serializable
    res.foreach(println)








  }

}

