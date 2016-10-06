package edu.dataart.kolhun.task8

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession


object StackOverflowStatistic {


  case class Post(_Id: String, _Tags: String)


  case class PostTransformed(id: Int, tagIndex: Int, value: Int)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "file://")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._
    val postsDS = spark.read.json("src/main/resources/data/stack-overflow-snippet.json").as[Post]
      .filter(_._Tags != null)
      .flatMap(post => splitTags(post))


    val allTags = postsDS
      .map(_._Tags)


    val tagsToNumbers: Map[Int, Array[String]] = allTags.collect()
      .groupBy(tag => tag.hashCode)


    val transformedPosts = postsDS
      .map(post => PostTransformed(post._Id.toInt, post._Tags.hashCode(), 1))
    val ratingsRdd = transformedPosts.map(post => Rating(post.id, post.tagIndex, post.value.toDouble)).rdd
    val featuresNumber = 50
    val numIterations = 10
    val model = ALS.train(ratingsRdd, featuresNumber, numIterations, 0.01)
    val postTags = ratingsRdd.map { case Rating(post, tag, value) =>
      (post, tag)
    }
    val predictedRdd = model.predict(postTags)

    predictedRdd
      .map { case Rating(post, tag, value) =>
        ((post, tag), value)
      }
      .map(
        prediction => (prediction._1._1, tagsToNumbers(prediction._1._2)(0), prediction._2))

      .sortBy(tuple => (tuple._1, tuple._3), ascending = false)
      .collect().foreach(println)

  }


  private def splitTags(post: Post): Seq[Post] = {
    val regex = "<[^<>]+>".r
    regex.findAllMatchIn(post._Tags)
      .map(charSeq => Post(post._Id, charSeq.toString()))
      .toSeq

  }


}