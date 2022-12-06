package crystal.Sql.Spark

import org.apache.log4j._
import org.apache.spark.sql._



object SparkSQLDataset {
  case class Person(id:Int,
                   name:String,
                    age:Int,
                    friends:Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._
    val schemaPeople = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\HP\\IdeaProjects\\ScalaDataProc\\SparkSql_DataFrames_DataSet\\src\\main\\scala\\crystal\\Sql\\Spark\\friends.csv")
      .as[Person]

    schemaPeople.printSchema()
//    schemaPeople.createOrReplaceGlobalTempView("people")
//
//    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 25")
//
//    val results = teenagers.collect()
//
//    results.foreach(println)


 //   schemaPeople.select("name").show()
    schemaPeople.groupBy("age").count().show()

//    schemaPeople.filter(schemaPeople("age")<21).show()
//
//    schemaPeople.select(schemaPeople("name"),schemaPeople("age")+10).show()

    spark.stop()

  }

}
