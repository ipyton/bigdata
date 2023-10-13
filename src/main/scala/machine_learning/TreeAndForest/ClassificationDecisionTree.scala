package machine_learning.TreeAndForest

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ClassificationDecisionTree {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Classification ")
    conf.setMaster("local[3]") //

    val sc = new SparkContext(conf)
    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "src/main/resources/汽车数据样本.txt")
    // divide these records into test and training pieces.
    val splits = data.randomSplit(Array(0.7, 0.3))
    val(training_data, test_data) = (splits(0), splits(1))

    val numClasses = 2
    val categoricalInfo = Map[Int, Int](0->4, 1->4, 2->3, 3->3) //how many categories they want to have

    val impurity="entropy"
    val maxDepth = 3 //max depth of the tree
    // 连续值的离散化程度
    val maxBins = 32
    val model = DecisionTree.trainClassifier(training_data,numClasses,categoricalInfo,
      impurity,maxDepth,maxBins)

    // train
    val labelAndPredicts=test_data.map({point=>{
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }})

    val testErr = labelAndPredicts.filter(r=> r._1 != r._2).count().toDouble / test_data.count()
    println(model.toDebugString)





  }

}
