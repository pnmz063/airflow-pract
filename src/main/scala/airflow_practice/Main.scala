package airflow_practice

import org.apache.spark.sql.SparkSession
import scala.io.Source.fromURL
import java.io.FileWriter
import sys.process

object Main extends App{

  def fileDowloader(url: String, filename: String) {
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


  // fileDowloader("https://github.com/pnmz063/airflow-pract/raw/main/data/u.data", "scores.csv")
  // fileDowloader("https://github.com/pnmz063/airflow-pract/raw/main/data/u.item", "film_items.csv")

  val scores = spark.read.option("delimiter", "\\t").csv("./data/u.data")
  val film_items = spark.read.option("delimiter", "|").csv("./data/u.item")

  scores.printSchema()
  film_items.printSchema()

}

