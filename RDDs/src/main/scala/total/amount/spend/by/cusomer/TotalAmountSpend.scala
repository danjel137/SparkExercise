package total.amount.spend.by.cusomer

import org.apache.spark.SparkContext

object TotalAmountSpend {
def extractCustomerPriceParis(line:String):(Int,Float)={
  val fields=line.split(",")
  (fields(0).toInt,fields(2).toFloat)
}

  def main(args: Array[String]): Unit = {
    val sc=new SparkContext("local[*]","TotalAmountSpend")

    val input=sc.textFile("C:\\Users\\HP\\IdeaProjects\\ScalaDataProc\\RDDs\\src\\main\\scala\\total\\amount\\spend\\by\\cusomer\\customer.csv")
    val mappedInput=input.map(extractCustomerPriceParis)

    val totalByCustomer=mappedInput.reduceByKey((x,y)=>x+y)

    val flipped=totalByCustomer.map(x=>(x._2,x._1))

    val totalByCustomerSorted=flipped.sortByKey()

    val results=totalByCustomerSorted.collect()
    results.foreach(println)
  }
}
