package practice5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Solution_5 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local", "practice_4")
  val rdd1 = sc.textFile("C:\\Users\\Ultimatrix\\Desktop\\shared\\week_data\\moviedata-201008-180523.data")
  val rdd2 = rdd1.map(x => x.split("\t")(2))
  //val rdd3 = rdd2.map(x => (x,1))
  //val rdd4 = rdd3.reduceByKey((x,y)=>x+y).sortByKey()
  //val result = rdd4.collect
  //result.foreach(println)

  val result2 = rdd2.countByValue()
  result2.foreach(println)

}
