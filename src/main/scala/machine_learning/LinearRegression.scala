package machine_learning

import org.apache.spark.SparkConf
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.SparkSession

object LinearRegression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    val spark = SparkSession.builder().config(conf).appName("LinearRegression").getOrCreate()

    val data = spark.read.format("libsvm").load("src/main/resources/sample_linear_regression_data.txt")

    val DFS = data.randomSplit(Array(0.8, 0.2), 1)
    val (training, test) = (DFS(0), DFS(1))

    val lr = new LinearRegression()
    lr.setMaxIter(10)
    lr.setTol(1) // convergence tolerance

    //part 2 :optimization :if the parameters of an model is too big, it may means the model is over-fitting and limiting that is necessary
    //l1 : scale down the model
    //l2 : consider the most effective way (5->3 , 3->1)
    lr.setElasticNetParam(0.8) // used to adjust the proportion of L1 and L2
    lr.setRegParam(0.8) //the sum of the L1 and L2

    // fit the model
    val lrModel = lr.fit(training)

    // get the result
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    val predictionAndLabel = lrModel.setFeaturesCol("features").setPredictionCol("test_prediction").transform(test)
    val loss = predictionAndLabel.rdd.map(row => {
      val label = row.getAs[Double]("label")
      val predict = row.getAs[Double]("test_prediction")
      Math.abs(label - predict)
    }).reduce(_ + _)

    val error = loss / test.count

    //save the model
    val savePath = "./model"
    lrModel.write.overwrite().save(savePath)

    //show some information about the error
    val summary = lrModel.summary
    println(summary.rootMeanSquaredError)


    //load the model
    val regressionModel = LinearRegressionModel.load(savePath)
    regressionModel.transform(test)
    spark.read.parquet(savePath + "/data").show(false)


  }
}
