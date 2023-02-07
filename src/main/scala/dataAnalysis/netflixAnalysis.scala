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
    .transform(transformStringDate("effective_date", "effective_date", "release_year"))
    .transform(transformCountry("country"))
    .drop(col("date_added"))


//EDA

  //Quantos filmes/séries foram lançados por ano?
  //Qual dia que mais teve filmes/séries adicionadas na netflix?
  //Quantidade de filmes/séries adicionados por dia

  val show_type = Seq("Movie", "TV Show")

  for (show_types <- show_type) {
    data
    .transform(showsReleasedPerYear(show_types))
    //.show()

    data
      .transform(maxShowPerDay(show_types))
      //.show()

    data
      .transform(showsPerPeriod(show_types))
      //.show()
  }

  //

  //data.select(col("country")).distinct().orderBy("country").show( 1000, false)



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

  def transformStringDate(column_selected: String, column_transformed: String, new_column_type: String)(df: DataFrame): DataFrame = {
    df
      .withColumn(column_transformed, to_date(col(column_selected)))
      .withColumn(new_column_type, col(new_column_type).cast("int"))
  }

  def transformCountry(column_selected: String)(df: DataFrame): DataFrame = {
    val nonCountries = Seq("Aziz Ansari", "Chuck D.", "Dominic Costa", "Doug Plaut", "Francesc Orella",
      "Justin \"\"Alyssa Edwards\"\" Johnson", "Lachion Buckingham", "Leonardo Sbaraglia", "Michael Cavalieri",
      "Rob Morgan", "Tantoo Cardinal", "Tobechukwu \"\"iLLbliss\"\" Ejiofor")

    df
      .withColumn(column_selected,
        when(trim(col(column_selected)).isin(nonCountries:_*), null)
      .otherwise(trim(col(column_selected))))
  }

  def showsPerPeriod(show_type: String)(df: DataFrame): DataFrame = {
    df
      .groupBy(col("effective_date"))
      .agg(countDistinct(col("show_id")).as(s"Count ${show_type}"))
      .orderBy(col("effective_date").desc)
  }

  def showsReleasedPerYear(show_type: String)(df: DataFrame): DataFrame = {
    df
      .filter(col("type").equalTo(show_type))
      .groupBy(col("release_year"))
      .agg(countDistinct(col("show_id")).as(s"Total Quantity ${show_type}"))
      .orderBy(col(s"Total Quantity ${show_type}").desc)
  }

  def maxShowPerDay(show_type: String)(df: DataFrame): DataFrame = {
    df
      .filter(col("type").equalTo(show_type))
      .groupBy(col("effective_date"))
      .agg(count(col("show_id")).as(s"Max ${show_type} Added in a Day"))
      .orderBy(col(s"Max ${show_type} Added in a Day").desc)
      .limit(1)
  }

}
