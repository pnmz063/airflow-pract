"""
package airflow_practice

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.FileWriter

object Main extends App{

  def fileDownloader(url: String, filename: String) {
    val src = scala.io.Source.fromURL(url)
    val out = new FileWriter(s"./$filename")
    out.write(src.mkString)
    out.close()
  }



  implicit val spark: SparkSession = SparkSession.builder()
    .appName("spark_app")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()


   // fileDownloader("https://raw.githubusercontent.com/pnmz063/airflow-pract/main/data/books.csv", "./books.csv")

  val books: DataFrame = spark.read.option("header", "true").csv("/data/books.csv")

  import spark.implicits._


  books
    .filter($"average_rating" >= "4.50")
    .write.format("orc")
    .mode("overwrite")
    .saveAsTable("airflow_practice_db.the_best_books")
}
"""

