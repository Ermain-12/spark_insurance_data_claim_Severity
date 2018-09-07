package org.ermain.scala.spark.allstate_datat_analysis

object EDA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionCreate.createSession()

    import spark.implicits._
    val df = Data_Preprocessing.trainInput
    df.show()
  }
}
