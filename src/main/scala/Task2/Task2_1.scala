package Task2
import common.CommonUtils.{getSparkSession, readDataFrame}
object Task2_1 {

  val spark = getSparkSession("Task2_1- Reading Airbnb data frame")

  def main(args: Array[String]): Unit = {

    var inputFile = "src/main/resources/data/sf-airbnb-clean.parquet"
    var outFile = "out/out2_1.txt"

    if (args.length > 0) {
      inputFile = if (args(0).isEmpty) throw new IllegalArgumentException("input not provided") else args(0)
      outFile = if (args(1).isEmpty) throw new IllegalArgumentException("input not provided") else args(1)
    }

    val airbnbDF = readDataFrame(spark, inputFile)

  }

}
