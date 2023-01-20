package airflow_practice

import org.apache.spark.sql.SparkSession

object Main extends App{
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("spark_app")
    .master("yarn")
    .enableHiveSupport()
    .getOrCreate()
}

