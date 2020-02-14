package com.dama.spark.streaming

import com.dama.spark.streaming.networkStream.{lines, ssc}
import com.dama.spark.util.Constants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileStream extends App {
  lazy val sparkConf = new SparkConf().setAppName("Learn Spark").setMaster("local[*]").set("spark.cores.max", "2")
  val ssc = new StreamingContext(sparkConf, Seconds(10))

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  rootLogger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val lines = ssc.textFileStream("H:\\STudy\\test\\streamFiles\\*")

  lines.flatMap(line => line.split(" ")).map(word =>(word,1)).reduceByKey(_ + _).print()
  ssc.start()
  ssc.awaitTermination()

}
