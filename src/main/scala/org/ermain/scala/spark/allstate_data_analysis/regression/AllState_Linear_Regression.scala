package org.ermain.scala.spark.allstate_data_analysis.regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.Row
import org.ermain.scala.spark.allstate_datat_analysis.{Data_Preprocessing, SparkSessionCreate}

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
    val pipeline = new Pipeline()
      .setStages(Array(Data_Preprocessing.assembly, model))

    // We now perform the K-fold Cross-Validation and Grid-Search to find the best
    // parameters for the algorithm
    println("Preparing K-Fold Cross-Validation and Grid-Search: Model tuning")

    val parameterGrid = new ParamGridBuilder()
      .addGrid(model.maxIter, MaxIter)
      .addGrid(model.regParam, RegParam)
      .addGrid(model.tol, Tol)
      .addGrid(model.elasticNetParam, ElasticNetParam)
      .build()

    val cross_validator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(parameterGrid)
      .setNumFolds(numFolds)

    // Train the model with a linear regression algorithm
    println("Training model with the Linear Regression algorithm....")

    val cv_Model = cross_validator.fit(Data_Preprocessing.trainingData)

    // We can now save the work-flow
    cv_Model.write.overwrite.save("models/Logistic_Regression_model")

    // Now, we can load the work-flow back in to evaluate the data
    val same_cv_Model = CrossValidatorModel.load("models/Logistic_Regression_model")

    // **********************************************************************
    println("Evaluating model on train and validation set and calculating RMSE")
    // **********************************************************************

    val trainPredictionsAndLabels = cv_Model.transform(Data_Preprocessing.trainingData)
      .select("label", "prediction")
      .map{
        case Row(label: Double, prediction: Double)  => (label, prediction)
      }.rdd

    val validationPredictionsAndLabels = cv_Model.transform(Data_Preprocessing.validationData)
      .select("label", "prediction")
      .map {
        case Row(label: Double, prediction: Double)   => (label, prediction)
      }.rdd


    val trainingDataRegressionMetrics = new RegressionMetrics(trainPredictionsAndLabels)
    val validationDataRegressionMetrics = new RegressionMetrics(validationPredictionsAndLabels)
    val bestModel = cv_Model.bestModel.asInstanceOf[Pipeline]


    val results = "\n=====================================================================\n" +
      s"Param trainSample: ${Data_Preprocessing.trainSample}\n" +
      s"Param testSample: ${Data_Preprocessing.testSample}\n" +
      s"TrainingData count: ${Data_Preprocessing.trainingData.count}\n" +
      s"ValidationData count: ${Data_Preprocessing.validationData.count}\n" +
      s"TestData count: ${Data_Preprocessing.testData.count}\n" +
      "=====================================================================\n" +
      s"Param maxIter = ${MaxIter.mkString(",")}\n" +
      s"Param numFolds = $numFolds\n" +
      "=====================================================================\n" +
      s"Training data MSE = ${trainingDataRegressionMetrics.meanSquaredError}\n" +
      s"Training data RMSE = ${trainingDataRegressionMetrics.rootMeanSquaredError}\n" +
      s"Training data R-squared = ${trainingDataRegressionMetrics.r2}\n" +
      s"Training data MAE = ${trainingDataRegressionMetrics.meanAbsoluteError}\n" +
      s"Training data Explained variance = ${trainingDataRegressionMetrics.explainedVariance}\n" +
      "=====================================================================\n" +
      s"Validation data MSE = ${validationDataRegressionMetrics.meanSquaredError}\n" +
      s"Validation data RMSE = ${validationDataRegressionMetrics.rootMeanSquaredError}\n" +
      s"Validation data R-squared = ${validationDataRegressionMetrics.r2}\n" +
      s"Validation data MAE = ${validationDataRegressionMetrics.meanAbsoluteError}\n" +
      s"Validation data Explained variance = ${validationDataRegressionMetrics.explainedVariance}\n" +
      s"CV params explained: ${cv_Model.explainParams}\n" +
      // s"GBT params explained: ${bestModel.stages.last.asInstanceOf[LinearRegressionModel].explainParams}\n" +
      "=====================================================================\n"
    println(results)
  }
}
