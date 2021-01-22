package Task2

import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.{DataFrame, SaveMode}
import common.CommonUtils.{getSparkSession, readDataFrame}

object Task2_4 {

  val spark = getSparkSession("Task2_4- Airbnb price and review rating")


  // to get the lowest price and max review
  def getResult(airbnbDF: DataFrame) = {
    val filterCondition = airbnbDF
      .select(min(col("price")).alias("lowest_price"),
        max(col("review_scores_rating")).alias("max_review"))
      .head()

    val lowest_price = filterCondition.get(0)
    val max_review = filterCondition.get(1)


    val result = airbnbDF.
      where(s"price = ${lowest_price} and review_scores_rating = ${max_review}")
      .select(col("accommodates"))
    //.select(col("accommodates"),col("price"),col("review_scores_rating"))

    result
  }


  def writeToFile(airbnbDF: DataFrame, path: String) = {


    val result = getResult(airbnbDF)
    //result.show()

    result.write
      .format("csv")
      .option("header", true)
      .option("sep", ",")
      .mode(SaveMode.Overwrite)
      .save(path)
  }


  def main(args: Array[String]): Unit = {

    var inputFile = "src/main/resources/data/sf-airbnb-clean.parquet"
    var outFile = "out/out2_4.txt"

    if (args.length > 0) {
      inputFile = if (args(0).isEmpty) throw new IllegalArgumentException("input path not provided") else args(0)
      outFile = if (args(1).isEmpty) throw new IllegalArgumentException("output path not provided") else args(1)
    }

    val airbnbDF = readDataFrame(spark, inputFile)
    writeToFile(airbnbDF, outFile)
  }

}
