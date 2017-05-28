package com.dk.spark.algo.topn

import com.dk.spark.context.SparkContextManager
import org.apache.spark.rdd.RDD

/**
  * Created by Divakant Pandey on 5/28/17.
  */

case class Tup(url: String, hitCount: Int) {
  override def toString = (hitCount, url).toString()
}

/**
  * Implicit Ordering created for Tuples, Similar to Comparator in Java
  *
  **/
object Tup {

  val orderByHit = Ordering.by { x: Tup =>
    x.hitCount
  }

  val orderByUrl = Ordering.by { x: Tup =>
    x.url
  }

}

object TopNForNonUniqueKeysUsingTakeOrdered {

  val N = 10
  val sc = SparkContextManager.getContext()
  val broadcastTopN = sc.broadcast(N)

  def main(args: Array[String]): Unit = {
    val input = sc.textFile("src/main/resources/topn_non_unique_keys/").filter(x => x != "")
    topN(input)
  }

  def topN(input: RDD[String]) = {

    //Create Partition
    val rdd = input.coalesce(9)

    // Make Unique keys
    val pairs = rdd.map(x => {
      val t = x.split(",")
      (t(0), t(1).toInt)
    }).reduceByKey(_ + _).map(f => new Tup(f._1, f._2))


    //val finalTopN = pairs.top(10)

    //Order by Hit
    val finalTopN = pairs.takeOrdered(10)(Tup.orderByHit.reverse)

    finalTopN.foreach(println)
  }

}
