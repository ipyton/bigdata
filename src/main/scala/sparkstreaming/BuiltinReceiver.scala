package sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object BuiltinReceiver {
  def main(args: Array[String]): Unit = {
    // one of the thread is used to receiver job. Another is used to calculate. so 2 is the minimum request.
    val conf = new SparkConf().setAppName("internal receiver").setMaster("local[*]")


    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")


    //set the source of the
    val dataDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8889) //receive from

    val res:DStream[(String, String)] = dataDStream.map(_.split(" ")).map(
      vars => {
        Thread.sleep(20000)
        (vars(0), vars(1))
      }
    )

    res.print()

    ssc.start()
    ssc.awaitTermination()

  }

  def userReceiver(): Unit ={
    val conf: SparkConf = new SparkConf().setAppName("custom receiver").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    val dstream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomReceiver("localhost", 8889))

    dstream.print()

    ssc.start()
    ssc.awaitTermination()



  }





}
