package dataAnalysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col


object netflixAnalysis extends App {

  val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("Netflix Analysis")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val path = "C:/data-analysis/netflix_titles.csv"

  val data = readingFile(path)
    .transform(transformDate("date_added","new_date"))
    .filter(col("new_date").isNotNull)
    .transform(getMonth("new_date", "month"))
    .transform(getDay("new_date", "day"))
    .transform(getYear("new_date", "year"))
    .transform(concatDateColumns("effective_date", "year", "month", "day"))
    .transform(transformStringDate("effective_date", "effective_date"))
    .drop(col("date_added"))

  data.show()


  def readingFile(path: String): DataFrame = {
    val df = spark
      .read
      .option("delimiter", ",")
      .option("header", "true")
      .csv(path)
    df
  }

  def checkingUniqueValues(column_selected: String)(df: DataFrame): DataFrame = {
    df
      .select(column_selected).distinct().groupBy(column_selected).count().orderBy(desc(column_selected))
  }

  def transformDate(column_selected: String, new_column: String)(df: DataFrame): DataFrame = {
    df
      .withColumn(new_column, when(size(split(col(column_selected), " ")) === 3, col(column_selected))
        .otherwise(null))
  }

  def getMonth(column_selected: String, new_column: String)(df: DataFrame): DataFrame = {
    val extractMonth = udf((date: String) => date.split(" ")(0))
    df
      .withColumn(new_column, extractMonth(col(column_selected)))
      .withColumn(new_column, to_date(col(new_column), "MMMM"))
      .withColumn(new_column, month(col(new_column)))
  }

  def getDay(column_selected: String, new_column: String)(df: DataFrame): DataFrame = {
    val extractDay = udf((date: String) => date.split(" ")(1))
    val removeChar = udf((element: String) => element.replace(",", ""))

    df
      .withColumn(new_column, extractDay(col(column_selected)))
      .withColumn(new_column, removeChar(col(new_column)).cast("int"))
  }

  def getYear(column_selected: String, new_column: String)(df: DataFrame): DataFrame = {
    val extractYear = udf((date: String) => date.split(" ")(2))
    df
      .withColumn(new_column, extractYear(col(column_selected)))
  }

  def concatDateColumns(new_date_column: String, year_column: String, month_column: String, day_column: String)(df: DataFrame):DataFrame = {
    df
      .withColumn(new_date_column,
        concat_ws("-", col(year_column), col(month_column), col(day_column)))
  }

  def transformStringDate(column_selected: String, column_transformed: String)(df: DataFrame): DataFrame = {
    df
      .withColumn(column_transformed, to_date(col(column_selected)))
  }

}
