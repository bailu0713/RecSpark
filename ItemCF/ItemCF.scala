package com.ctvit.movielens

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by BaiLu on 2015/4/3.
 */
object ItemCF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Movie")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("hdfs://lab01:8020/user/bl/movielens/ml-10m/ratings.dat")
    val ratings = rdd.map { line => val field = line.split("::"); (field(0), field(1), field(2))}
    val numRaterPerMovie = ratings.groupBy(tup => tup._2).map { tup => (tup._1, tup._2.size)}


    //    groupBy(tup=>tup._2)返回结果格式(3283,CompactBuffer((1473,3283,3), (1564,3283,3), (1741,3283,3),(5074,3283,4), (5077,3283,4), (5848,3283,3), (5990,3283,4)))
    //    groupBy(tup=>tup._2).join(numRaterPerMovie)返回结果格式(13,(CompactBuffer((38,13,3), (49,13,4), (75,13,4), (114,13,4), (536,13,4), (541,13,2)),99))
    //    flatMap 返回结果(10,2,5,701)    (13,2,3,701)    (18,2,2,701)

    val ratingsWithSize = rdd.map { line => val field = line.split("::"); (field(0), field(1), field(2))}.groupBy(tup => tup._2).join(numRaterPerMovie).flatMap { joined => joined._2._1.map(f => (f._1, f._2, f._3, joined._2._2))}
    //        ratingsWithSize 返回结果格式(user,item,rating,itemcount)
    //    keyBy(tup=>tup._1)  (user,item,ratings)=>(user,(user,item,ratings))
    //    (13,(13,2,3,701))

    val ratings2 = ratingsWithSize.keyBy(tup => tup._1)

    //    join()返回结果
    //    val tup1=sc.parallelize(Seq((1, "A"), (2, "B"), (3, "C"),(2,"D")))
    //    val tup2=sc.parallelize(Seq((1, "Z"), (1, "ZZ"), (2, "Y"),(2,"E"), (3, "X")))
    //    tup1.join(tup2)
    //    Array[(Int, (String, String))] = Array((1,(A,Z)), (1,(A,ZZ)), (2,(B,Y)), (2,(B,E)), (2,(D,Y)), (2,(D,E)), (3,(C,X)))
    //计算同一个user看过的所有的item的相似度
    val ratingPairs = ratingsWithSize.keyBy(tup => tup._1).join(ratings2).filter(f => f._2._1._2 < f._2._2._2)
    //    ratingPairs=(user,((user,item1,ratings1,count1),(user,item2,ratings2,count2)))
    val vectorCalcs = ratingPairs.map(data => {
      val key = (data._2._1._2, data._2._2._2)
      val stats =
        (data._2._1._3.toDouble * data._2._2._3.toDouble, // rating 1 * rating 2
          data._2._1._3.toDouble, // rating movie 1
          data._2._2._3.toDouble, // rating movie 2
          math.pow(data._2._1._3.toDouble, 2), // square of rating movie 1
          math.pow(data._2._2._3.toDouble, 2), // square of rating movie 2
          data._2._1._4, // number of raters movie 1
          data._2._2._4) // number of raters movie 2
      (key, stats) //((item1,item2),(rating1*rating2,rating1,rating2,rating1^2,rating2^2,numrater1,numrater1))
    }).groupByKey().map(data => {
      val key = data._1
      val vals = data._2
      val size = vals.size
      val dotProduct = vals.map(f => f._1).sum // rating 1 * rating 2
      val ratingSum = vals.map(f => f._2).sum // rating movie 1
      val rating2Sum = vals.map(f => f._3).sum // rating movie 2
      val ratingSq = vals.map(f => f._4).sum // square of rating movie 1
      val rating2Sq = vals.map(f => f._5).sum // square of rating movie 2
      val numRaters = vals.map(f => f._6).max // number of raters movie 1
      val numRaters2 = vals.map(f => f._7).max // number of raters movie 2
      (key, (size, dotProduct, ratingSum, rating2Sum, ratingSq, rating2Sq, numRaters, numRaters2))
    })

    val PRIOR_COUNT = 10
    val PRIOR_CORRELATION = 0
    // compute similarity metrics for each movie pair
    val similarities = vectorCalcs.map(fields => {
      val key = fields._1
      val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, numRaters, numRaters2) = fields._2
      val corr = correlation(size.toDouble, dotProduct.toDouble, ratingSum.toDouble, rating2Sum.toDouble, ratingNormSq.toDouble, rating2NormSq.toDouble)
      val regCorr = regularizedCorrelation(size.toDouble, dotProduct.toDouble, ratingSum.toDouble, rating2Sum.toDouble,
        ratingNormSq.toDouble, rating2NormSq.toDouble, PRIOR_COUNT.toDouble, PRIOR_CORRELATION.toDouble)
      val cosSim = cosineSimilarity(dotProduct.toDouble, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq))
      val jaccard = jaccardSimilarity(size.toDouble, numRaters.toDouble, numRaters2.toDouble)
      (key, (corr, regCorr, cosSim, jaccard))
    })

    val outputPath="hdfs://192.168.168.41:8020/user/bl/ItemCF"
    val hadoopConf=new Configuration()
    val hadoopfs=FileSystem.get(hadoopConf)
    if (hadoopfs.exists(new Path(outputPath)))
      hadoopfs.delete(new Path(outputPath),true)
    similarities.saveAsTextFile(outputPath)

  }


  // *************************
  // * SIMILARITY MEASURES
  // *************************

  /**
   * The correlation between two vectors A, B is
   * cov(A, B) / (stdDev(A) * stdDev(B))
   *
   * This is equivalent to
   * [n * dotProduct(A, B) - sum(A) * sum(B)] /
   * sqrt{ [n * norm(A)^2 - sum(A)^2] [n * norm(B)^2 - sum(B)^2] }
   */
  def correlation(size: Double, dotProduct: Double, ratingSum: Double,
                  rating2Sum: Double, ratingNormSq: Double, rating2NormSq: Double) = {
    val numerator = size * dotProduct - ratingSum * rating2Sum
    val denominator = scala.math.sqrt(size * ratingNormSq - ratingSum * ratingSum) * scala.math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)
    numerator / denominator
  }

  /**
   * Regularize correlation by adding virtual pseudocounts over a prior:
   * RegularizedCorrelation = w * ActualCorrelation + (1 - w) * PriorCorrelation
   * where w = # actualPairs / (# actualPairs + # virtualPairs).
   */
  def regularizedCorrelation(size: Double, dotProduct: Double, ratingSum: Double,
                             rating2Sum: Double, ratingNormSq: Double, rating2NormSq: Double,
                             virtualCount: Double, priorCorrelation: Double) = {
    val unregularizedCorrelation = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
    val w = size / (size + virtualCount)
    w * unregularizedCorrelation + (1 - w) * priorCorrelation
  }

  /**
   * The cosine similarity between two vectors A, B is
   * dotProduct(A, B) / (norm(A) * norm(B))
   */
  def cosineSimilarity(dotProduct: Double, ratingNorm: Double, rating2Norm: Double) = {
    dotProduct / (ratingNorm * rating2Norm)
  }

  /**
   * The Jaccard Similarity between two sets A, B is
   * |Intersection(A, B)| / |Union(A, B)|
   */
  def jaccardSimilarity(usersInCommon: Double, totalUsers1: Double, totalUsers2: Double) = {
    val union = totalUsers1 + totalUsers2 - usersInCommon
    usersInCommon / union
  }


}
