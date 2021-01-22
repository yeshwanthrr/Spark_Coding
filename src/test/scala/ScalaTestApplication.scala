import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ScalaTestApplication extends AnyFlatSpec with should.Matchers {
  "getAllGroceries.show(1)" should "return a value from groceries.csv" in {
    val allGroceries = Task1.Task1_2A.getAllGroceries
    allGroceries.take(1).foreach(println)
  }

  "getAllGroceries.count()" should " return 169" in {
    val allProducts = Task1.Task1_2B.getAllGroceries.count()
    println(assert(allProducts == 169))
  }

  "TopGroceries" should " return top 5 groceries" in {
    val topGroceries = Task1.Task1_3.getTopGroceries.take(1).map(x => x._1)
    )topGroceries.foreach(println)

  }

  "Task2_2 getresult" should "fetch min, max and total count of records" in {
    val spark = common.CommonUtils.getSparkSession("Test Task2_2")
    val airbnb =  common.CommonUtils.readDataFrame(spark,"src/main/resources/data/sf-airbnb-clean.parquet")
    val result = Task2.Task2_2.getResult(airbnb).show()
  }

  "Task2_3 getresult" should "average bedroom and avg bedroom for the property costing > 5000" in {
    val spark = common.CommonUtils.getSparkSession("Test Task2_3")
    val airbnb =  common.CommonUtils.readDataFrame(spark,"src/main/resources/data/sf-airbnb-clean.parquet")
    val result = Task2.Task2_3.getResult(airbnb).show()
  }

  "Task2_4 getresult" should "number of people the apartment can accomodate with high rating and lowest cost" in {
    val spark = common.CommonUtils.getSparkSession("Test Task2_4")
    val airbnb =  common.CommonUtils.readDataFrame(spark,"src/main/resources/data/sf-airbnb-clean.parquet")
    val result = Task2.Task2_4.getResult(airbnb).show()
  }

}
