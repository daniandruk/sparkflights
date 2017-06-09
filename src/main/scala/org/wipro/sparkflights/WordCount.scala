package org.wipro.sparkflights

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]) {
    
    val inputFile = getClass.getResource("/airlines.dat").toURI().toString()
    
    //Configuration for a Spark application.        
    val conf = new SparkConf()
    conf.setAppName("WordCount").setMaster("local")

    //Create Spark Context  
    val sc = new SparkContext(conf)

    //Create MappedRDD by reading from HDFS file from path command line parameter  
    val rdd = sc.textFile(inputFile)

    //WordCount  
    rdd.flatMap(_.split(" ")).
      map((_, 1)).
      reduceByKey(_ + _).
      map(x => (x._2, x._1)).
      sortByKey(false).
      map(x => (x._2, x._1)).
      saveAsTextFile("SparkWordCountResult")

    //stop context
    sc.stop
  }
}