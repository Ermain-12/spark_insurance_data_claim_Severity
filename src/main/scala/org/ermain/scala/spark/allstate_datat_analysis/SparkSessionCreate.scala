package org.ermain.scala.spark.allstate_datat_analysis

import java.io.File

import org.apache.spark.sql.SparkSession

object SparkSessionCreate {

  def createSession(): SparkSession = {
    val wareHouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("all_state_severity_prediction")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", wareHouseLocation)
      .getOrCreate()

    spark
  }
}
