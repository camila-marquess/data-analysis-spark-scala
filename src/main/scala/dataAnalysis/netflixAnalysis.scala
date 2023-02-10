package dataAnalysis

import org.apache.spark.sql.{SparkSession}
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
      //.show()

    data
      .transform(showsPerDuration(show_types))
      //.show()

    data
      .transform(topGenresPerShow(show_types))
      //.show()
  }
}
