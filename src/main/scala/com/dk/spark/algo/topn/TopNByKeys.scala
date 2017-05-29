package com.dk.spark.algo.topn

import com.dk.spark.context.SparkContextManager
import org.apache.spark.rdd.RDD

/**
  * Created by Divakant Pandey on 5/29/17.
  */
object TopNByKeys {
  val N = 2
  val sc = SparkContextManager.getContext()
  val broadcastTopN = sc.broadcast(N)

  def main(args: Array[String]): Unit = {
    val input = sc.textFile("src/main/resources/topn_non_unique_keys/").filter(x => x != "")
    topNByKeys(input)
  }

  def topNByKeys(input: RDD[String]) = {

    //Create Partition
    val coalesceP = input.coalesce(9)

    //Create Pair RDD
    val pairs = coalesceP.map(_.split(",")).map(x => (x(0), x(1).toInt))

    //Aggregate By Pair --> Output RDD[(String, List[Int])]
    val aggregatedPairs = pairs.aggregateByKey(List[Int]())(_ ++ List(_), _ ++ _)

    //Convert , Sort and limit the List values
    val sortedPairs = aggregatedPairs.mapValues(_.toList.sortWith(_ > _).take(broadcastTopN.value))

    sortedPairs.foreach(println)
  }
}
