package flatmap

import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    val sc=new SparkContext("local[*]","WordCount")

    val input=sc.textFile("C:\\Users\\HP\\IdeaProjects\\ScalaDataProc\\RDDs\\src\\main\\scala\\flatmap\\textRandom.csv")

    //val words=input.flatMap(x=>x.split(" "))
    val words=input.flatMap(x=>x.split("\\W+"))
    val word=words.map(x=>x.toLowerCase())

    //println(words)
   // val wordd=word.countByValue()

   // wordd.foreach(println)
    val WordCounts=word.map(x=>(x,1)).reduceByKey((x,y)=>x+y)

    val wordCountSorted=WordCounts.map(x=>(x._2,x._1)).sortByKey()

    for(result<-wordCountSorted){
      val count=result._1
      val word=result._2
      println(s"$word: $count")
    }
  }
}
