package edu.dataart.kolhun.task3

import org.apache.spark.{SparkConf, SparkContext}


object UserVisitsCountryStatistic {

  val WORD_DELIMITER = ","
  val COUNTRY_LINE_INDEX = 5


  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .set("spark.master", "local")
      .setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("src/main/resources/data/uservisits/*")
    val countriesToCounter = textFile
      .map(mapToCountry)
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    countriesToCounter
      .take(10)
      .foreach(println)
  }


  private def mapToCountry(line: String): (String, Long) = {
    val words = line.split(WORD_DELIMITER)
    (words(COUNTRY_LINE_INDEX), 1)
  }
}
