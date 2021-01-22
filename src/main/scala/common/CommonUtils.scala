package common

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object CommonUtils {

  def getSparkSession(name: String,runLocal:Boolean = true): SparkSession = {

    if(runLocal) {
      val spark = SparkSession
        .builder()
        .appName(name)
        .config("spark.master", "local[2]")
        .getOrCreate()

      spark
    }
    else {
      val spark = SparkSession.builder().appName(name).getOrCreate()
      spark
    }

  }

  def readGroceriesData(sc: SparkContext, path: String) = {

    sc.textFile(path)
  }

  def readDataFrame(spark: SparkSession, path: String) = {

    spark.read.parquet(path)
  }


}
