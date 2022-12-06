package filtered.temperature

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
object FilterTemp {
  def parseLine(line:String):(String,String,Float)={
    val fields=line.split(",")
    val stationID=fields(0)
    val entryType=fields(2)
    val temperature=fields(3).toFloat*0.1f*(9.0f/5.0f)+32.0f
    (stationID,entryType,temperature)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc=new SparkContext("local[*]","FilterTemp")

    val lines=sc.textFile("C:\\Users\\HP\\IdeaProjects\\ScalaDataProc\\src\\main\\scala\\filteredtemperature\\temperature.csv")

    val parsedLines=lines.map(parseLine)

    val minTemp=parsedLines.filter(x=>x._2=="TEMP")

    val stationTEmp=minTemp.map(x=>(x._1,x._3.toFloat))

    val minTempByStation=stationTEmp.reduceByKey( (x,y)=>math.min(x,y) )

    val results=minTempByStation.collect()

    for(result<-results.sorted){
      val station=result._1
      val temp=result._1
      val formattedTemp=temp
      println("---------------")
      println(s"$station minimum temperature: $formattedTemp")
      println("---------------")

      //      println(temp)
//      println(formattedTemp)
    }

  }

}
