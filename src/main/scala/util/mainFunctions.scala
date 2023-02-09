package util

import dataAnalysis.netflixAnalysis.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc, month, size, split, to_date, udf, when}

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
}
