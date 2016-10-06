package edu.dataart.kolhun.task5

import org.apache.spark.sql.SparkSession

/**
  * Created by akolgun on 8/11/2016.
  */
object VacanciesStatistic {


  case class Vacancy(name: String)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "file://")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val peopleDS = spark.read.json("src/main/resources/data/hh-vacs.json")

    peopleDS.createOrReplaceTempView("vacancies")

    fetchDevelopersMeanSalaryByCity(spark)
    fetchDesignersMeanSalaryByCity(spark)
    fetchAdminsMeanSalaryByCity(spark)

  }

  private def fetchDevelopersMeanSalaryByCity(spark: SparkSession) {
    println("Getting developers statistic...")
    val developersDF = spark.sql(
      "SELECT  address.city, avg(salary.from)" +
        "FROM vacancies " +
        "WHERE salary.from is not null " +
        "AND address.city is not null " +
        "AND (UPPER(name) like '%ПРОГРАММИСТ%' " +
        "OR UPPER(name) like '%DEVELOPER%' " +
        "OR UPPER(name) like '%PROGRAMMER%')  " +
        "GROUP BY address.city " +
        "ORDER BY avg(salary.from) DESC")

    developersDF.collect().foreach(println)
  }

  private def fetchDesignersMeanSalaryByCity(spark: SparkSession) {
    println("Getting designers statistic...")
    val designersDF = spark.sql(
      "SELECT  address.city, avg(salary.from)" +
        "FROM vacancies " +
        "WHERE salary.from is not null " +
        "AND address.city is not null " +
        "AND (UPPER(name) like '%ДИЗАЙНЕР%' " +
        "OR UPPER(name) like '%DESIGNER%')  " +
        "GROUP BY address.city " +
        "ORDER BY avg(salary.from) DESC")

    designersDF.collect().foreach(println)
  }

  private def fetchAdminsMeanSalaryByCity(spark: SparkSession) {
    println("Getting admins statistic...")
    val adminsDF = spark.sql(
      "SELECT  address.city, avg(salary.from)" +
        "FROM vacancies " +
        "WHERE salary.from is not null " +
        "AND address.city is not null " +
        "AND (UPPER(name) like '%АДМИНИСТРАТОР%' " +
        "OR UPPER(name) like '%ADMINISTRATOR%') " +
        "GROUP BY address.city " +
        "ORDER BY avg(salary.from) DESC")

    adminsDF.collect().foreach(println)
  }

}
