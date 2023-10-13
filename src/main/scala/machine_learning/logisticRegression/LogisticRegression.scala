package machine_learning.logisticRegression

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.SparkSession

object LogisticRegression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("dd").setMaster("local[4]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._ // a tool to convert the spark objects to dataframe

    //方差归一化
    /* val standardScaler = new StandardScaler()
     standardScaler.setInputCol("features")
     standardScaler.setOutputCol("new_features")
     standardScaler.setWithMean(true)
     standardScaler.setWithStd(true)
     val scalerModel = standardScaler.fit(data)
     val dataFrame = scalerModel.transform(data)
     dataFrame.show(100,false)*/



    val dataRDD = spark.sparkContext.textFile("src/main/resources/breast_cancer.csv")
    val data = dataRDD.map(x=> {
      val arr = x.split(",")
      val features = new Array[String](arr.length - 1)

      arr.copyToArray(features, 0, arr.length - 1)
      val label = arr(arr.length - 1)
      (new DenseVector(features.map(_.toDouble)), label.toDouble) // divide features and labels
    }).toDF("features", "label")

    val splits = data.randomSplit(Array(0.7, 0.3), seed=11L)
    val (trainingData, testData) = (splits(0), splits(1))

    val lr = new LogisticRegression()
    lr.setMaxIter(10)
    lr.setFitIntercept(true)
    val lrModel = lr.fit(trainingData)  // training
    println(s"Coefficients: ${lrModel.coefficients}  Intercept: ${lrModel.intercept}")

    val test = lrModel.transform(testData)

    test.show(false)


    //calculate the right rate
    val mean = test.rdd.map(row => {
      val label = row.getAs[Double]("label")
      val prediction = row.getAs[Double]("prediction")

      //0: right, 1: false
      math.abs(label-prediction)
    }).sum()


    //customize the classifying value
    val count = test.rdd.map(row => {
      val probability = row.getAs[DenseVector]("probability")
      val label = row.getAs[Double]("label")
      val score = probability(1)
      val prediction = if(score > 0.3) 1 else 0
      math.abs(label - prediction)
    }).sum()

    println("right rate" + (1 - (count/testData.count())))
    spark.close()

  }
}
