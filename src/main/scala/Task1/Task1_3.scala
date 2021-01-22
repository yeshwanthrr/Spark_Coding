package Task1

import common.CommonUtils.{getSparkSession, readGroceriesData}
import java.io.{BufferedWriter, File, FileWriter}

object Task1_3  {

  var inputFile = "src/main/resources/data/groceries.csv"
  var outFile = "out/out1_3.txt"

  val spark = getSparkSession("Top 5 groceries")
  val sc = spark.sparkContext
  val groceriesRDD = readGroceriesData(sc,inputFile)


  def getTopGroceries = groceriesRDD.flatMap(row => row.split(","))
    .map(product => (product, 1))
    .reduceByKey(_ + _)
    .map(row => (row._2, row._1))
    .sortByKey(false, 1)
    .map(row => (row._2, row._1))
    .take(5)

  def writeRDDResult(outFile:String) = {
    val top5Groceries = getTopGroceries

    val file = new File(outFile)
    val bufferedWriter = new BufferedWriter(new FileWriter(file))
    top5Groceries.foreach(tup => bufferedWriter.write(s"(" + tup._1 + "," + tup._2 + ")" + "\n"))
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
