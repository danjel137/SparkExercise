package coutWords

import breeze.linalg.split
import breeze.optimize.DiffFunction.castOps
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountBetterSortedDataset {

  case class Book(value:String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

//    import spark.implicits._
//    val input=spark.read
//      .text("C:\\Users\\HP\\IdeaProjects\\ScalaDataProc\\SparkSql_DataFrames_DataSet\\src\\main\\scala\\coutWords\\book.csv")
//      .as[Book]
//
//    val words=input
//      .select(explode(split($"value","\\W+")).alias("word"))
//      .filter($"word"=!="")


    val book =spark.sparkContext.textFile("C:\\Users\\HP\\IdeaProjects\\ScalaDataProc\\SparkSql_DataFrames_DataSet\\src\\main\\scala\\coutWords\\book.csv")
    val wordsRDD=book.flatMap(x=>x.split("\\W+"))
//    val wordsDS=wordsRDD.toDS()
//    val lowercaseWordsDS=wordsRDD.select
  }



}
