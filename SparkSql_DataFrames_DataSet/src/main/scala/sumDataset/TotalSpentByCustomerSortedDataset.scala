package sumDataset

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{round, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

object TotalSpentByCustomerSortedDataset {
  case class CustomerOrders(cust_id:Int,item_id:Int,amount_spent:Double)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession
      .builder()
      .appName("TotalSpentByCustomer")
      .master("local[*]")
      .getOrCreate()

    val customerOrdersSchema=new StructType()
      .add("cust_id",IntegerType,nullable = true)
      .add("item_id",IntegerType,nullable = true)
      .add("amount_spent",DoubleType,nullable = true)

    import spark.implicits._

    val customerDS =spark.read
      .schema(customerOrdersSchema)
      .csv("C:\\Users\\HP\\IdeaProjects\\ScalaDataProc\\SparkSql_DataFrames_DataSet\\src\\main\\scala\\sumDataset\\customers.csv")
      .as[CustomerOrders]


    val totalByCustomer = customerDS
      .groupBy("cust_id")
      .agg(round(sum("amount_spent"), 2)
        .alias("total_spent"))

 //   totalByCustomer.show()

    val totalByCustomerSorted = totalByCustomer.sort("total_spent")

    totalByCustomerSorted.show(totalByCustomer.count.toInt)

  }

}
