package Task2

import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.{DataFrame, SaveMode}
import common.CommonUtils.{getSparkSession, readDataFrame}

object Task2_3 {

  val spark = getSparkSession("Task2_3- Airbnb Averaging")

  //average bathrooms and bedrooms
  def getResult(airbnbDF: DataFrame) = {
    val result = airbnbDF
      .where("price > 5000 and review_scores_value = 10")
      .select(
        avg(col("bathrooms")).alias("avg_bathrooms"),
        avg(col("bedrooms")).alias("avg_bedrooms"))

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
    var outFile = "out/out2_3.txt"

    if (args.length > 0) {
      inputFile = if (args(0).isEmpty) throw new IllegalArgumentException("input path not provided") else args(0)
      outFile = if (args(1).isEmpty) throw new IllegalArgumentException("output path not provided") else args(1)
    }

    val airbnbDF = readDataFrame(spark, inputFile)
    writeToFile(airbnbDF, outFile)
  }

}
