package org.ermain.scala.spark.allstate_datat_analysis

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
    .format("org.databricks.spark.csv")
    .load(train)
    .cache()

  // Load and format the testing data
  val testInput = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .format("org.databricks.spark.csv")
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


}
