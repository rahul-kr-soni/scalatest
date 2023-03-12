package word_count
import org.apache.spark.SparkContext

object first_object extends App {
 val sc = new SparkContex("local[*]","word_count")

  val line = sc.textFile("src/main/word_count/contextcheck.txt")

}
