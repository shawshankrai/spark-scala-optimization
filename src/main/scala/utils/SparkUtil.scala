package utils

import org.apache.spark.sql.SparkSession

object SparkUtil {

  def getSparkSession(appName: String, master: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName(appName)
      .master(master)
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    spark
  }
}
