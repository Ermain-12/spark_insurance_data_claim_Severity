package org.ermain.scala.spark.allstate_datat_analysis

import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.ml.feature.VectorAssembler

object Data_Preprocessing {

  val trainSample = 1.0
  val testSample = 1.0
  val train = "data/train.csv"
  val test = "data/test.csv"

  val spark = SparkSessionCreate.createSession()

  import spark.implicits._
  println(s"Reading data from $train file")

  // Format the training data
  val trainInput = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .format("com.databricks.spark.csv")
    .load(train)
    .cache

  // Load and format the testing data
  val testInput = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .format("com.databricks.spark.csv")
    .load(test)
    .cache

  println("Preparing data for training model")

  var data = trainInput.withColumnRenamed("loss", "label")
    .sample(false, trainSample)

  val DF = data.na.drop()

  // Check to see if any inputs are null
  if (data == DF ){
    println("No null values in the data frame")
  }else{
    println("Null values are present in this data-frame")
    data = DF
  }

  val seed = 12345L
  val splits = data.randomSplit(Array(0.75, 0.25), seed)
  val (trainingData, validationData) = (splits(0), splits(1))

  // Cache th data for faster in-memory processing
  trainingData.cache
  validationData.cache

  val testData = testInput.sample(false, testSample).cache

  def isCategory(s: String): Boolean = s.startsWith("cat")
  def categoryNewColumn(s: String): String = {
    if (isCategory(s)) {
      s"idx_$s"
    }else{
      s
    }
  }

  // This function removes certain categories that are unnecessary for the model
  def removeTooManyCategories(s: String): Boolean = {
    !(s matches "cat(109$|110$|112$|113$|116$)" )
  }

  // Now we select the feature columns
  def onlyFeatureColumns(s: String): Boolean = {
    !(s matches "id|label")
  }

  // We now define the set feature columns
  val featureCols = trainingData.columns
    .filter(removeTooManyCategories)
    .filter(onlyFeatureColumns)
    .map(categoryNewColumn)

  // We use the string indexer for type String categorical columns
  val stringIndexerStages = trainingData.columns.filter(isCategory)
      .map(s => new StringIndexer()
      .setInputCol(s)
      .setOutputCol(categoryNewColumn(s))
      .fit(trainInput.select(s).union(testInput.select(s))))

  // Use the VectorAssembler for training features
  val assembly = new VectorAssembler()
    .setInputCols(featureCols)
    .setOutputCol("features")
}
