package machine_learning.weibo_recommendation

import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.{IDF, IDFModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.wltea.analyzer.lucene.IKAnalyzer
import org.apache.spark.mllib.linalg.Vector

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


//构建出来一个app支持简单有效的内存管理
object BlogKMeans {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BlogKMeans").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("src/main/resources/weibo/original.txt",8)
    var wordRDD: RDD[(String, ArrayBuffer[String])] = rdd.mapPartitions(iterator => {
        val list = new ListBuffer[(String, ArrayBuffer[String])]
        while (iterator.hasNext) {
          val analyzer = new IKAnalyzer(true)
          val line = iterator.next()
          val strings = line.split("\t")
          val (id, text) = (strings(0),strings(1))
          val ts:TokenStream = analyzer.tokenStream("", text)

          //get the result of analyzer
          val term: CharTermAttribute = ts.getAttribute(classOf[CharTermAttribute])
          ts.reset()
          val arr = new ArrayBuffer[String]
          while (ts.incrementToken()) {
            arr.+=(term.toString())
          }
          list.append((id, arr))
          analyzer.close()
        }
        list.iterator
    })
    wordRDD = wordRDD.cache()
    //the differences between cache and persist
    val hashingTF:HashingTF = new HashingTF(1000)

    // becomes a vector contains times that words appears such as [3,37,126,159,215,230,243,408,413,423,450,500,528,563,596,627,767,810,958,974],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,3.0,1.0,1.0,1.0]
    val tfRDD: RDD[(String, Vector)] = wordRDD.map(x => {
      (x._1, hashingTF.transform(x._2))
    }) //get the tf


    val idf: IDFModel = new IDF().fit(tfRDD.map(_._2)) // compute the idf
    val tfIdfs: RDD[(String, Vector)] = tfRDD.mapValues(idf.transform(_)) // idf + tf = tf-idf
    tfIdfs.foreach(println)//(3793627656408402,(1000,[29,40,81,139,215,334,552,572,629,750,969,989],[2.5842834076491195,2.0393937591086377,2.887626845795233,1.949311645483251,0.0041828418467634776,3.851398669783831,2.5018875187525182,1.9325386982615442,1.8913941751140777,0.4466818475987985,4.264447406102095,4.049503469903619]))


    val kcluster = 20
    val kmeans = new KMeans()
    kmeans.setK(kcluster)

    kmeans.setInitializationMode("k-means||")
    kmeans.setMaxIterations(1000)  //max time of iteration

    // train the data
    val kmeansModel: KMeansModel = kmeans.run(tfIdfs.map(_._2))

    // broadcast it to other nodes
    val model_broadcast = sc.broadcast(kmeansModel)
    //key: weibo ID, value: class
    val predictionRDD: RDD[(String, Int)] = tfIdfs.mapValues(vector => {
      val model = model_broadcast.value
      model.predict(vector)
    })

    //sum the result of prediction
    //tfIdfs2wordsRDD:kv格式的RDD
    //key:weibo ID
    //value: tuple2 (Vector(tfidf1,tfidf2....),ArrayBuffer(word,word,word....))
    //
    val TF_IDF_to_words_RDD: RDD[(String, (Vector, ArrayBuffer[String]))] = tfIdfs.join(wordRDD) // a and b has a same key


//    * result:KV
//    * K：微博ID
//    * V:(类别号，(Vector(tfidf1,tfidf2....),ArrayBuffer(word,word,word....)))
    val result: RDD[(String, (Int, (Vector, ArrayBuffer[String])))] = predictionRDD.join(TF_IDF_to_words_RDD)

    result.filter(x=>x._2._1 == 2).flatMap(line=>{ //GET THE CLASS 2's tf-idf and word
      val tfIdf: Vector = line._2._2._1
      val words: ArrayBuffer[String] = line._2._2._2
      val list = new ListBuffer[(Double, String)]

      for (i <- 0 until words.length) {
        list.append((tfIdf(hashingTF.indexOf(words(i))), words(i)))
      }
      list
    }).sortBy(x => x._1, false)
      .map(_._2).distinct()
      .take(30).foreach(println)
    sc.stop()
  }

}
