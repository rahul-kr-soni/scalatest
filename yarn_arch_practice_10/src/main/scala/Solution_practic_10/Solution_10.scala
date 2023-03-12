package Solution_practic_10

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Solution_10 extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local","solution_10")

  val my_list = List("ERROR: Thu Jun 04 10:37:51 BST 2015",
    "WARN: Sun Nov 06 10:37:51 GMT 2016",
    "WARN: Mon Aug 29 10:37:51 BST 2016",
    "ERROR: Thu Dec 10 10:37:51 GMT 2015",
    "ERROR: Fri Dec 26 10:37:51 GMT 2014",
    "ERROR: Thu Feb 02 10:37:51 GMT 2017",
    "WARN: Fri Oct 17 10:37:51 BST 2014",
  )

  //val rdd1 = sc.parallelize(my_list)

  val rdd1 = sc.textFile("C:\\Users\\Ultimatrix\\Desktop\\shared\\week_10\\bigLog.txt")

  //my way
  // val rdd2 = rdd1.map(x => {
  //(x.split(":")(0),1)
  //})

  // other by one by one

  val rdd2 = rdd1.map(x => {
    val columns = x.split(":")
    val level = columns(0)
    (level,1)
  })

  val rdd3 = rdd2.reduceByKey((x,y)=>x+y)
  rdd3.collect.foreach(println)

}
//github push test