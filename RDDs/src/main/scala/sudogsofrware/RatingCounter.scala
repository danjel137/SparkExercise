package sudogsofrware

import org.apache.spark._
import org.apache.log4j._
object RatingCounter extends App {
  //Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","RatingCounter")

  val lines=sc.textFile("C:\\Users\\HP\\IdeaProjects\\ScalaDataProc\\src\\main\\scala\\sudogsofrware\\data.csv")

  val ratings=lines.map(x=>x.split(",")(2))

  val results =ratings.countByValue()

 // val sortedResults=results.toSeq.sortBy(_._1)
  println("------------------")

  results.foreach(println)
  println("------------------")
}
