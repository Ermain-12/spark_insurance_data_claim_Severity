package org.ermain.scala.spark.allstate_datat_analysis

object EDA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionCreate.createSession()

    import spark.implicits._
    val df = Data_Preprocessing.trainInput
    df.show()

    //Let's show some seected column only. But feel free to use DF.show() to see the all columns.
    df.select("id", "cat1", "cat2", "cat3", "cont1", "cont2", "cont3", "loss").show()

    //If you see all the rows sing df.show() you will see some categorical columns contains too many categories.
    df.select("cat109", "cat110", "cat112", "cat113", "cat116").show()

    println(df)
    println(df.printSchema())
  }
}
