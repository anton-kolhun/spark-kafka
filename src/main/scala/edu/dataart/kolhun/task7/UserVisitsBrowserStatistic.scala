package edu.dataart.kolhun.task7

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Properties

import eu.bitwalker.useragentutils.UserAgent
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.springframework.jdbc.datasource.embedded.{EmbeddedDatabaseBuilder, EmbeddedDatabaseType}


object UserVisitsBrowserStatistic {

  val dataSource = new EmbeddedDatabaseBuilder()
    .setType(EmbeddedDatabaseType.H2)
    //.addScript("sql/schema.sql")
    .build


  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "file://")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val userVisitsRdd = spark.sparkContext.textFile("src/main/resources/data/uservisits/*")
    val schemaString = "date browser"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = false))
    val schema = StructType(fields)
    val rowRDD = userVisitsRdd
      .map(_.split(","))
      .map(attributes => Row(convertToMonth(attributes(2)), convertToUserAgent(attributes(4))))

    val userVisitsDf = spark.createDataFrame(rowRDD, schema)
    userVisitsDf.createOrReplaceTempView("uservisits")
    val results = spark.sql(
      "SELECT browser as browser , date, count(*) as number " +
        "FROM uservisits " +
        "GROUP BY browser, date")

    saveToDB(results)
    readFromDB(spark)
  }

  private def convertToUserAgent(userAgentStr: String): String = {
    UserAgent.parseUserAgentString(userAgentStr).getBrowser.toString
  }

  private def convertToMonth(dateStr: String): String = {
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    //LocalDate.parse(dateStr, dateFormatter).getYear.toString + "-" +
    LocalDate.parse(dateStr, dateFormatter).getMonth.toString
  }

  private def saveToDB(results: DataFrame) {
    val connProperties = new Properties()
    connProperties.put("user", "sa")
    results.write.jdbc("jdbc:h2:mem:testdb", "user_visit", connProperties)
  }

  private def readFromDB(spark: SparkSession) {

    val jdbcDF = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:h2:mem:testdb",
        "dbtable" -> "user_visit", "user" -> "sa")).load()

    jdbcDF.createOrReplaceTempView("user_visit")

    val designersDF = spark.sql(
      "SELECT * FROM user_visit ORDER BY browser, number DESC")
    designersDF.foreach(println(_))

  }


}

