package com.dk.spark.context

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Divakant Pandey on 5/28/17.
  */
object SparkContextManager {
  val conf: SparkConf = new SparkConf().setAppName("topN").setMaster("local").setSparkHome("/home/rock/spark-2.0.2-bin-hadoop2.7/")
  val sc: SparkContext = new SparkContext(conf)

  def getContext() = sc
}
