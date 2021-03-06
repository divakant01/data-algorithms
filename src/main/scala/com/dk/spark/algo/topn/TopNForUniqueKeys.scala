package com.dk.spark.algo.topn

import com.dk.spark.context.SparkContextManager

import scala.collection.immutable.TreeMap
/**
  * Created by Divakant Pandey on 5/28/17.
  */
object TopNForUniqueKeys {

  val N=10
  val sc = SparkContextManager.getContext()
  val broadcastTopN=sc.broadcast(N)

  def main(args: Array[String]): Unit = {
    val input = sc.textFile("src/main/resources/topn.txt").filter(x => x != "")
    val pairs = input.map(x => {
      val t = x.split(",");
      (t(1), t(0))
    })
    val partitions = pairs.mapPartitions(word => sortByPartition(word))
    partitions.collect().foreach(println)
  }

  def sortByPartition(word: Iterator[(String, String)]): Iterator[(String, String)] = {
    var top10 = new TreeMap[String, String]
    while(word.hasNext){
      val tup=word.next()
      top10 += (tup._2 -> tup._1)
      if(top10.size>broadcastTopN.value){
        //
        top10-=top10.firstKey
        // For Descending order --> top10-=top10.lastKey
      }
    }
    top10.iterator
  }

}
