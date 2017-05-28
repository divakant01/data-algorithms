package com.dk.spark.algo

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.TreeMap

/**
  * Created by Divakant Pandey on 5/28/17.
  */
object TopN {

  val N=10
  val conf: SparkConf = new SparkConf().setAppName("topN").setMaster("local").setSparkHome("/home/rock/spark-2.0.2-bin-hadoop2.7/")
  val sc: SparkContext = new SparkContext(conf)

  val broadcastTopN=sc.broadcast(N)

  def sortByPartition(word:Iterator[(Int,String)]):Iterator[(Int,String)]={
    var top10=new TreeMap[Int,String]
    while(word.hasNext){
      val tup=word.next()
      top10+=(tup._1->tup._2)
      if(top10.size>broadcastTopN.value){
        //
        top10-=top10.firstKey
        // For Descending order --> top10-=top10.lastKey
      }
    }
    top10.iterator
  }

  def main(args: Array[String]): Unit = {
    val input=sc.textFile("src/main/resources/topn.txt").filter(x=>x!="")
    val pairs=input.map(x=>{val t=x.split(",");(t(0).toInt,t(1))})
    val partitions=pairs.mapPartitions(word=>sortByPartition(word))
    partitions.collect().foreach(println)
  }

}