package sparkstreaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

class CustomReceiver(host:String, port:Int) extends Receiver[String](StorageLevel.DISK_ONLY) {

  //when it starts it start to read from host and port
  override def onStart(): Unit = {
    def ooxx(): Unit = {
      val server = new Socket(host,port)

      val reader = new BufferedReader(new InputStreamReader(server.getInputStream))
      var line: String = reader.readLine()
      while(!isStopped()  &&  line != null ){
        store(line)
        line  = reader.readLine()
      }
    }

    new Thread{
      override def run(): Unit = {
        ooxx()
      }
    }.start()
  }

  override def onStop(): Unit = {

  }
}
