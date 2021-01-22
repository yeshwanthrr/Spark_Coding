package Task1

import common.CommonUtils.{getSparkSession, readGroceriesData}


object Task1_1 {

  //Path initialization, for testing in local- also works by sending run time arguments
  var inputFile = "src/main/resources/data/groceries.csv"
  var outFile = "out/out1_1.txt"

  val spark = getSparkSession("Reading Groceries RDD")
  val sc = spark.sparkContext
  val groceriesRDD = readGroceriesData(sc,inputFile)
  def showGroceriesList = groceriesRDD.map(x => List(x)).take(5).foreach(println)

  def main(args: Array[String]): Unit = {

    if (args.length > 0) {
      inputFile = if (args(0).isEmpty) throw new IllegalArgumentException("input not provided") else args(0)
      outFile = if (args(1).isEmpty) throw new IllegalArgumentException("input not provided") else args(1)
    }

    showGroceriesList


  }









}
