package com.dk.spark.algo.secondarysort

import com.dk.spark.context.SparkContextManager

/**
  * Created by Divakant Pandey on 5/28/17.
  */
object SortByValue {

  val sc = SparkContextManager.getContext()


  def main(args: Array[String]): Unit = {
    val input = sc.textFile("src/main/resources/sort.txt").filter(x => x != "")
    input.map(x => {
      val t = x.split(",")
      (t(0), (t(1), t(2)))
    }).
      groupByKey().
      mapValues(_.toList.sortBy(_._1)).foreach(println)
  }

}
