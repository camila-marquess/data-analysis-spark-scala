package util

import dataAnalysis.netflixAnalysis.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, count, countDistinct, desc, month, size, split, to_date, trim, udf, when}

object mainFunctions {

  /**
   * Method used to read CSV file
   * @param path
   * @return
   */
  def readingFile(path: String): DataFrame = {
    val df = spark
      .read
      .option("delimiter", ",")
      .option("header", "true")
      .csv(path)
    df
  }

  /**
   * Method used to get unique values from a column
   * @param column_selected
   * @param df
   * @return
   */
  def checkingUniqueValues(column_selected: String)(df: DataFrame): DataFrame = {
    df
      .select(column_selected).distinct().groupBy(column_selected).count().orderBy(desc(column_selected))
  }

  /**
   * Method used in order to get only date
   * @param column_selected
   * @param new_column
   * @param df
   * @return
   */
  def transformDate(column_selected: String, new_column: String)(df: DataFrame): DataFrame = {
    df
      .withColumn(new_column, when(size(split(col(column_selected), " ")) === 3, col(column_selected))
        .otherwise(null))
  }

  /**
   * Method used to extract month from date column
   * @param column_selected
   * @param new_column
   * @param df
   * @return
   */
  def getMonth(column_selected: String, new_column: String)(df: DataFrame): DataFrame = {
    val extractMonth = udf((date: String) => date.split(" ")(0))
    df
      .withColumn(new_column, extractMonth(col(column_selected)))
      .withColumn(new_column, to_date(col(new_column), "MMMM"))
      .withColumn(new_column, month(col(new_column)))
  }

  /**
   * Method used to extract day from date column
   * @param column_selected
   * @param new_column
   * @param df
   * @return
   */
  def getDay(column_selected: String, new_column: String)(df: DataFrame): DataFrame = {
    val extractDay = udf((date: String) => date.split(" ")(1))
    val removeChar = udf((element: String) => element.replace(",", ""))

    df
      .withColumn(new_column, extractDay(col(column_selected)))
      .withColumn(new_column, removeChar(col(new_column)).cast("int"))
  }

  /**
   * Method used to extract year from date
   * @param column_selected
   * @param new_column
   * @param df
   * @return
   */
  def getYear(column_selected: String, new_column: String)(df: DataFrame): DataFrame = {
    val extractYear = udf((date: String) => date.split(" ")(2))
    df
      .withColumn(new_column, extractYear(col(column_selected)))
  }

  /**
   * Method used to concatenate date columns
   * @param new_date_column
   * @param year_column
   * @param month_column
   * @param day_column
   * @param df
   * @return
   */
  def concatDateColumns(new_date_column: String, year_column: String, month_column: String, day_column: String)(df: DataFrame):DataFrame = {
    df
      .withColumn(new_date_column,
        concat_ws("-", col(year_column), col(month_column), col(day_column)))
  }

  /**
   * Method used to transform string to date type
   * @param column_selected
   * @param column_transformed
   * @param new_column_type
   * @param df
   * @return
   */
  def transformStringDate(column_selected: String, column_transformed: String, new_column_type: String)(df: DataFrame): DataFrame = {
    df
      .withColumn(column_transformed, to_date(col(column_selected)))
      .withColumn(new_column_type, col(new_column_type).cast("int"))
  }

  /**
   * Method used to get the main movie/tv show genre
   * @param column_selected
   * @param new_column
   * @param df
   * @return
   */
  def getMainGenre(column_selected: String, new_column: String)(df: DataFrame): DataFrame = {
    val extractGenre = udf((genre: String) => genre.split(",")(0))
    df
      .withColumn(new_column, extractGenre(col(column_selected)))
  }

  /**
   * Method used to clean country column
   * @param column_selected
   * @param df
   * @return
   */
  def transformCountry(column_selected: String)(df: DataFrame): DataFrame = {
    val nonCountries = Seq("Aziz Ansari", "Chuck D.", "Dominic Costa", "Doug Plaut", "Francesc Orella",
      "Justin \"\"Alyssa Edwards\"\" Johnson", "Lachion Buckingham", "Leonardo Sbaraglia", "Michael Cavalieri",
      "Rob Morgan", "Tantoo Cardinal", "Tobechukwu \"\"iLLbliss\"\" Ejiofor")

    df
      .withColumn(column_selected,
        when(trim(col(column_selected)).isin(nonCountries:_*), null)
          .otherwise(trim(col(column_selected))))
  }

  /**
   * Method used to clean duration column
   * @param column_selected
   * @param df
   * @return
   */
  def transformDurationColumn(column_selected: String)(df: DataFrame): DataFrame = {
    val nonDuration = Seq("Alan Cumming", "Benn Northover", "Donnell Rawlings", "Itziar Aizpuru", "Jimmy Herman\"",
      "MC Eiht", "Maurice Everett\"", "Sharon Ooja\"", "Wanda Sykes", "United States", "1994")

    df
      .withColumn(column_selected,
        when(trim(col(column_selected)).isin(nonDuration:_*), null)
          .otherwise(trim(col(column_selected))))
  }

  /**
   * Method used to count distinct movies/tv shows per day
   * @param show_type
   * @param df
   * @return
   */
  def showsPerPeriod(show_type: String)(df: DataFrame): DataFrame = {
    df
      .filter(col("type").equalTo(show_type))
      .groupBy(col("effective_date"))
      .agg(countDistinct(col("show_id")).as(s"Count ${show_type}"))
      .orderBy(col("effective_date").desc)
  }

  /**
   * Method used to count distinct movies/tv shows per released year
   * @param show_type
   * @param df
   * @return
   */
  def showsReleasedPerYear(show_type: String)(df: DataFrame): DataFrame = {
    df
      .filter(col("type").equalTo(show_type))
      .groupBy(col("release_year"))
      .agg(countDistinct(col("show_id")).as(s"Total Quantity ${show_type}"))
      .orderBy(col(s"Total Quantity ${show_type}").desc)
  }

  /**
   * Method used to get day which had more movies/tv shows added
   * @param show_type
   * @param df
   * @return
   */
  def maxShowPerDay(show_type: String)(df: DataFrame): DataFrame = {
    df
      .filter(col("type").equalTo(show_type))
      .groupBy(col("effective_date"))
      .agg(count(col("show_id")).as(s"Max ${show_type} Added in a Day"))
      .orderBy(col(s"Max ${show_type} Added in a Day").desc)
      .limit(1)
  }

  /**
   * Method used to count movies/tv shows per country
   * @param show_type
   * @param df
   * @return
   */
  def showsPerCountry(show_type: String)(df: DataFrame): DataFrame = {
    df
      .filter(col("type").equalTo(show_type))
      .groupBy(col("country"))
      .agg(countDistinct(col("show_id")).as(s"Count ${show_type}"))
      .orderBy(col(s"Count ${show_type}").desc)
  }

  /**
   * Method used to count movies/tv shows per duration
   * @param show_type
   * @param df
   * @return
   */
  def showsPerDuration(show_type: String)(df: DataFrame): DataFrame = {
    df
      .filter(col("type").equalTo(show_type))
      .groupBy(col("duration"))
      .agg(countDistinct(col("show_id")).as(s"Count ${show_type}"))
      .orderBy(col(s"Count ${show_type}").desc)
  }

  /**
   * Method used to get top 10 genres per movies/tv shows
   * @param show_type
   * @param df
   * @return
   */
  def topGenresPerShow(show_type: String)(df: DataFrame): DataFrame = {
    df
      .filter(col("type").equalTo(show_type))
      .groupBy(col("genre"))
      .agg(count(col("show_id")).as(s"Top 10 ${show_type} per Genre"))
      .orderBy(col(s"Top 10 ${show_type} per Genre").desc)
      .limit(10)
  }
}
