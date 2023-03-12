
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object practice_4 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local","practice_4")
  val rdd1= sc.textFile("C:\\Users\\Ultimatrix\\Desktop\\shared\\week_data\\customerorders-201008-180523.csv")
  val rdd2 = rdd1.map(x => (x.split(",")(0),x.split(",")(2).toFloat))
  val rdd3 = rdd2.reduceByKey((x,y)=>x+y)

  //sorting by value
  /*
  val rdd5 = rdd3.map(x => (x._2,x._1))   //making key as value and value as key
  val rdd6 = rdd5.sortByKey(false)  //now sort by key
  val rdd7 = rdd6.map(x =>(x._2,x._1))    //now again going to key to value and value to key
  */

  val rdd4 = rdd3.sortBy(x =>x._2,false)

  val result = rdd4.collect
  result.foreach(println)

  //scala.io.StdIn.readLine()

}
