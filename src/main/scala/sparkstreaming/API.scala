package sparkstreaming

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Duration, State, StateSpec, StreamingContext}

object API {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("testAPI")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir(".")
    //    sc.setCheckpointDir("hdfs://mycluster/ooxx/spark/sdfs")
    val ssc = new StreamingContext(sc,Duration(1000))  //最小粒度  约等于：  win：  1000   slide：1000

    val data: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8889)
    val mapdata: DStream[(String, Int)] = data.map(_.split(" ")).map(x=>(x(0),1))
    //state is used to make some calculation about the global
    val res: MapWithStateDStream[String, Int, Int, (String, Int)] = mapdata.mapWithState(StateSpec.function(
      (k: String, nv: Option[Int], ov: State[Int]) => {

        println(s"*************k:$k  nv:${nv.getOrElse(0)}   ov ${ov.getOption().getOrElse(0)}")
        (k, nv.getOrElse(0) + ov.getOption().getOrElse(0))
      }
    ))

    val rdd: RDD[Int] = sc.parallelize(1 to 10)
    while (true) {
      rdd.map(x=>{
        println("aaaaa")
        x
      }
      )
      Thread.sleep(1000)
    }

    val resource: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",8889)
    val format: DStream[(String, Int)] = resource.map(_.split(" ")).map(x=>(x(0),1))
    format.window(Duration(3000),Duration(2000))

    //ways converting dstream->RDD
    //scope of the variables: application, a job(per action), task/ work on every single record
    var bc: Broadcast[List[Int]] = sc.broadcast((1 to 5).toList) //application
    var jobNum = 0 //怎么能令jobNum的值随着job的提交执行，递增
    //    val res: DStream[(String, Int)] = format.filter(x=>{bc.value.contains(x._2)})
    println("aaaaaaa")  //application


    val res1: DStream[(String, Int)] = format.transform(rdd => {
          jobNum +=1  //RDD is calculated in driver so it can use the global variables
          println(s"jobNum: $jobNum")
          if(jobNum <=5){
            bc =  sc.broadcast((1 to 5).toList)
          }else{
            bc =  sc.broadcast((6 to 15).toList)
          }
          rdd.filter(x=>bc.value.contains(x._2)) //is calculated in executor
        })
    res1.print()

    ssc.start()
    ssc.awaitTermination()



  }

  def window(): Unit = {

    //distinguish slide window
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("testAPI")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir(".")

    val ssc = new StreamingContext(sc, Duration(1000))
    val resource: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8889)


    val format: DStream[(String, Int)] = resource.map(_.split(" ")).map(x => (x(0), 1))
    val res1: DStream[(String, Int)] = format.reduceByKey(_ + _)
    val res2: DStream[(String, Int)] = format.window(Duration(5000)).reduceByKey(_ + _)

    val reduce: DStream[(String, Int)] = format.reduceByKey(_ + _) //  窗口量是  1000  slide  1000
    val res3: DStream[(String, Int)] = reduce.window(Duration(5000))

    val win: DStream[(String, Int)] = format.window(Duration(5000)) //先调整量
    val res4: DStream[(String, Int)] = win.reduceByKey(_ + _) //在基于上一步的量上整体发生计算

    val res5: DStream[(String, Int)] = format.reduceByKeyAndWindow(_ + _, Duration(5000))

    val resource2: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8889)
    val format2: DStream[(String, Int)] = resource2.map(_.split(" ")).map(x => (x(0), x(1).toInt))
    //调优：
    //reduceByKey 对  combinationBykey的封装  放入函数，聚合函数，combina函数
    val res6: DStream[(String, Int)] = format2.reduceByKeyAndWindow(
      //计算新进入的batch的数据
      (ov: Int, nv: Int) => {
        println("first fun......")
        println(s"ov:$ov  nv:$nv")

        ov + nv
      }

      ,
      //挤出去的batch的数据
      (ov: Int, oov: Int) => {
        //        println("di 2 ge fun.....")
        //        println(s"ov:$ov   oov:$oov")
        ov - oov
      }
      ,
      Duration(6000), Duration(2000)
    )


    format.print()

    val res1s1batch: DStream[(String, Int)] = format.reduceByKey(_ + _)
    //    res1s1batch.mapPartitions(iter=>{println("1s");iter}).print()//打印的频率：1秒打印1次


    val newDS: DStream[(String, Int)] = format.window(Duration(5000), Duration(5000))

    val res5s5batch: DStream[(String, Int)] = newDS.reduceByKey(_ + _)
    res5s5batch.mapPartitions(iter => {
      println("5s");
      iter
    }).print() //打印频率：1秒打印1次}
  }

}
