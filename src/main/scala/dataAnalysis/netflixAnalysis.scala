package dataAnalysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import util.mainFunctions._



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
    .transform(transformDurationColumn("duration"))
    .transform(getMainGenre("listed_in", "genre"))
    .drop(col("date_added"))


//EDA

  //Quantos filmes/séries foram lançados por ano?
  //Qual dia que mais teve filmes/séries adicionadas na netflix?
  //Quantidade de filmes/séries adicionados por dia
  //Qual país teve mais filmes/séries produzidos pela netflix?
  //Quantidade de filmes/séries por duração

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

    data
      .transform(showsPerCountry(show_types))
      //.show(100, false)

    data
      .transform(showsPerDuration(show_types))
      //.show(100, false)

    data
      .transform(topGenresPerShow(show_types))
      .show(20)
  }

  //




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

  def getMainGenre(column_selected: String, new_column: String)(df: DataFrame): DataFrame = {
    val extractGenre = udf((genre: String) => genre.split(",")(0))
    df
      .withColumn(new_column, extractGenre(col(column_selected)))
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

  def transformDurationColumn(column_selected: String)(df: DataFrame): DataFrame = {
    val nonDuration = Seq("Alan Cumming", "Benn Northover", "Donnell Rawlings", "Itziar Aizpuru", "Jimmy Herman\"",
      "MC Eiht", "Maurice Everett\"", "Sharon Ooja\"", "Wanda Sykes", "United States", "1994")

    df
      .withColumn(column_selected,
        when(trim(col(column_selected)).isin(nonDuration:_*), null)
          .otherwise(trim(col(column_selected))))
  }

  def showsPerPeriod(show_type: String)(df: DataFrame): DataFrame = {
    df
      .filter(col("type").equalTo(show_type))
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

  def showsPerCountry(show_type: String)(df: DataFrame): DataFrame = {
    df
      .filter(col("type").equalTo(show_type))
      .groupBy(col("country"))
      .agg(countDistinct(col("show_id")).as(s"Count ${show_type}"))
      .orderBy(col(s"Count ${show_type}").desc)
  }

  def showsPerDuration(show_type: String)(df: DataFrame): DataFrame = {
    df
      .filter(col("type").equalTo(show_type))
      .groupBy(col("duration"))
      .agg(countDistinct(col("show_id")).as(s"Count ${show_type}"))
      .orderBy(col(s"Count ${show_type}").desc)
  }

  def topGenresPerShow(show_type: String)(df: DataFrame): DataFrame = {
    df
      .filter(col("type").equalTo(show_type))
      .groupBy(col("genre"))
      .agg(count(col("show_id")).as(s"Top 10 ${show_type} per Genre"))
      .orderBy(col(s"Top 10 ${show_type} per Genre").desc)
      .limit(10)
  }
}
