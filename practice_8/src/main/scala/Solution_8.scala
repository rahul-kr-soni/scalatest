import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.Source


object Solution_8 extends App {

    def loadBoringWord():Set[String] = {
        var boringWord:Set[String] = Set()
        //reading file local machine

        val lines = Source.fromFile("C:\\Users\\Ultimatrix\\Desktop\\shared\\week_10\\BoringWord.txt").getLines()


        // appending each line one by one
        for(line <- lines){
            boringWord += line
        }
        boringWord
    }

    //broadcasting to each node





    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local", "solution_7")


    //broadcasting boring word to each node``

    var stopword1 = sc.broadcast(loadBoringWord)

    val readfile = sc.textFile("C:\\Users\\Ultimatrix\\Desktop\\shared\\week_10\\bigdatacampaigndata-201014-183159.csv")
    //for (elem <- readfile.collect) {println(elem)}
    //readfile.collect.foreach(println)

    // fetching 1 and 11th column

    // val rdd2 = readfile.map(x => (x.split(",")(0),x.split(",")(10).toFloat))

    // key to value and value to key =====  val rdd3 = rdd2.map(x => (x._2,x._1))
    // reading in require way i.e 11 col will be key and 0th col will value so then we dont need to convert key to value and valuee to key
    val rdd2 = readfile.map(x => (x.split(",")(10).toFloat,x.split(",")(0)))

    //faltmap on value

    val rdd3 = rdd2.flatMapValues(x=>(x.split(" ")))

    //swaping key to value and value to key

    val rdd4 = rdd3.map(x => (x._2.toLowerCase(),x._1))

    //filtering out the stopwords before reduce by key

    val rdd5 = rdd4.filter(x => !stopword1.value(x._1))

    // aggregating

    val rdd6 = rdd5.reduceByKey((x,y) => x+y).sortBy(x => x._2, false)

    // print top 20 word
    rdd6.take(20).foreach(println)





    //rdd5.collect.foreach(println)







}
