package org.ermain.scala.spark.allstate_datat_analysis

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}

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
    .cache()

  // Load and format the testing data
  val testInput = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .format("com.databricks.spark.csv")
    .load(test)
    .cache()

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
  val splits = data.randomSplit(Array(0.85, 0.15), seed)
  val (trainingData, validationData) = (splits(0), splits(1))

  // Cache th data for faster in-memory processing
  trainingData.cache()
  validationData.cache()


  def isCategory(s: String): Boolean = s.startsWith("cat")
  def categoryNewColumn(s: String): String = {
    if (isCategory(s)) {
      s"idx_$s"
    }else{
      s
    }
  }

  // This function removes certain categories that are unnecessary for the model
  def removeCategory(s: String): Boolean = {
    ! (s matches "cat(109$|110$|112$|113$|116$)" )
  }

  // Now we select the feature columns
  def selectFeatureColumns(s: String): Boolean = {
    !(s matches "id|label")
  }

  // We now define the set feature columns
  val featureCols = trainingData.columns
    .filter(removeCategory)
    .filter(selectFeatureColumns)
    .map(categoryNewColumn)

  // We use the string indexer for type String categorical columns
  val stringIndexer = trainingData.columns.filter(isCategory)
    .map(c  => new StringIndexer()
      .setInputCol(c)
      .setOutputCol(categoryNewColumn(c))
      .fit(trainInput.select(c).union(testInput.select(c))))

  // Use the VectorAssembler for training features
  val assembly = new VectorAssembler()
    .setInputCols(featureCols)
    .setOutputCol("features")
}
