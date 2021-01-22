package Task1

import java.io.{BufferedWriter, File, FileWriter}
import common.CommonUtils.{getSparkSession, readGroceriesData}

object Task1_2A {

  //Path initialization, for testing in local- also works by sending run time arguments
  var inputFile = "src/main/resources/data/groceries.csv"
  var outFile = "out/out1_2a.txt"

  val spark = getSparkSession("Listing Grocery Products")
  val sc = spark.sparkContext
  val groceriesRDD = readGroceriesData(sc,inputFile)
  def showGroceriesList = groceriesRDD.map(x => List(x)).take(5).foreach(println)

  def getAllGroceries = groceriesRDD
    .flatMap(row => row.split(","))
    .distinct()

  def writeRDDResult(outFile:String) = {
    val allGroceries = getAllGroceries

    allGroceries.coalesce(1)
      .saveAsTextFile(outFile)
  }

  def main(args: Array[String]): Unit = {

    if (args.length > 0) {
      inputFile = if (args(0).isEmpty) throw new IllegalArgumentException("input not provided") else args(0)
      outFile = if (args(1).isEmpty) throw new IllegalArgumentException("input not provided") else args(1)
    }

    writeRDDResult(outFile)
  }



}
