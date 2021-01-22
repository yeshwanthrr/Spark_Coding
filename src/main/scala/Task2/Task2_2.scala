package Task2

import org.apache.spark.sql.functions.{col, count, max, min}
import org.apache.spark.sql.{DataFrame, SaveMode}
import common.CommonUtils.{getSparkSession, readDataFrame}

object Task2_2 {

  val spark = getSparkSession("Task2_2- airbnb-min-max-count")

  //calculate min_price, max_price and count
  def getResult(airbnbDF: DataFrame) = {
    val result = airbnbDF.select(
      min(col("price")).alias("min_price"),
      max(col("price")).alias("max_price"),
      count("*").alias("row_count")
    )

    result
  }

  def writeToFile(airbnbDF: DataFrame, path: String) = {

    val result = getResult(airbnbDF)

    result.write
      .format("csv")
      .option("header", true)
      .option("sep", ",")
      .mode(SaveMode.Overwrite)
      .save(path)
  }


  def main(args: Array[String]): Unit = {

    var inputFile = "src/main/resources/data/sf-airbnb-clean.parquet"
    var outFile = "out/out2_2.txt"

    if (args.length > 0) {
      inputFile = if (args(0).isEmpty) throw new IllegalArgumentException("input not provided") else args(0)
      outFile = if (args(1).isEmpty) throw new IllegalArgumentException("input not provided") else args(1)
    }

    val airbnbDF = readDataFrame(spark, inputFile)
    writeToFile(airbnbDF, outFile)
  }

}
