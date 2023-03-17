package airflow_practice

import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends App{

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("spark_app")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._


   // fileDownloader("https://raw.githubusercontent.com/pnmz063/airflow-pract/main/data/books.csv", "./books.csv")

  val file: DataFrame = spark.read.option("header", "true").csv("/Users/andrejkartasov/Downloads/secretar_202301301159.csv")
  file.printSchema()

}

