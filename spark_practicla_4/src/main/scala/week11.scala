import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object week11 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local", "practice_4")
  val rdd1 = sc.textFile("C:\\Users\\Ultimatrix\\Desktop\\shared\\week_data\\customerorders-201008-180523.csv")
  val rdd2 = rdd1.map(x => (x.split(",")(0), x.split(",")(2).toFloat))
  val rdd3 = rdd2.reduceByKey((x, y) => x + y)

  val rdd4 = rdd3.sortBy(x => x._2, false)
  val doubleAmaount = rdd4.map(x=>(x._1,x._2*2)).cache()

  rdd4.collect.foreach(println)
  println(doubleAmaount.count)

  // scala.io.StdIn.readLine()

}
