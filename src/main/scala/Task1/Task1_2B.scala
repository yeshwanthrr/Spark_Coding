package Task1

import java.io.{BufferedWriter, File, FileWriter}
import common.CommonUtils.{getSparkSession, readGroceriesData}

object Task1_2B{

  //Path initialization, for testing in local- also works by sending run time arguments
  var inputFile = "src/main/resources/data/groceries.csv"
  var outFile = "out/out1_2b.txt"

  val spark = getSparkSession("Listing Grocery Products")
  val sc = spark.sparkContext
  val groceriesRDD = readGroceriesData(sc,inputFile)

  def getAllGroceries = groceriesRDD
    .flatMap(row => row.split(","))
    .distinct()

  def writeRDDResult(outFile:String) = {
    val allGroceries = getAllGroceries
    val totalNoOfGroceries = allGroceries.count()
    val file = new File(outFile)
    val bufferedWriter = new BufferedWriter(new FileWriter(file))
    bufferedWriter.write("Count:" + "\n" + totalNoOfGroceries)
    bufferedWriter.close()
  }

  def main(args: Array[String]): Unit = {

    if (args.length > 0) {
      inputFile = if (args(0).isEmpty) throw new IllegalArgumentException("input not provided") else args(0)
      outFile = if (args(1).isEmpty) throw new IllegalArgumentException("input not provided") else args(1)
    }

    writeRDDResult(outFile)
  }

}
