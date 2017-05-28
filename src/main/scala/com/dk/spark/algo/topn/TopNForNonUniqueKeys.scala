package com.dk.spark.algo.topn

import com.dk.spark.context.SparkContextManager
import org.apache.spark.rdd.RDD

import scala.collection.immutable.TreeMap

/**
  * Created by Divakant Pandey on 5/28/17.
  */
object TopNForNonUniqueKeys {

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
      val t = x.split(",");
      (t(0), t(1).toInt)
    }).reduceByKey(_ + _)

    // Local topN
    val partitions = pairs.mapPartitions(word => sortByPartition(word))

    // finalTopN
    val array = partitions.collect()
    var finalTopN = new TreeMap[Int, String]()

    for (value <- array) {
      finalTopN += value
      if (finalTopN.size > N)
        finalTopN -= finalTopN.firstKey
    }

    finalTopN.foreach(println)
  }

  def sortByPartition(word: Iterator[(String, Int)]): Iterator[(Int, String)] = {
    var top10 = new TreeMap[Int, String]
    while (word.hasNext) {
      val tup = word.next()
      top10 += (tup._2 -> tup._1)
      if (top10.size > broadcastTopN.value) {
        //
        top10 -= top10.firstKey
        // For Descending order --> top10-=top10.lastKey
      }
    }
    top10.iterator
  }


}
