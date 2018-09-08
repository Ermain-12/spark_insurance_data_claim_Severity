package org.ermain.scala.spark.allstate_data_analysis.regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.regression.LinearRegression
import org.ermain.scala.spark.allstate_datat_analysis.SparkSessionCreate

object AllState_Linear_Regression {

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionCreate.createSession()

    import spark.implicits._
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    // Define the hyper-parameters for the regression ML-model
    val numFolds = 10
    val MaxIter: Seq[Int] = Seq(1000)
    val RegParam: Seq[Double] = Seq(0.001)
    val Tol: Seq[Double] = Seq(1e-6)
    val ElasticNetParam: Seq[Double] = Seq(0.001)


    // Create an estimator for the Linear Regression model
    val model = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")

    // Build the pipeline for transformations and predictors
  }
}
