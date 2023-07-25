package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    conf.set("spark.master","local[2]")
    conf.set("spark.app.name", "SparkStreamingApp")
    conf.set("spark.schedular.mode", "FAIR")

    val ssc = new StreamingContext(conf, Seconds(10))

//    val input = "/home/red/Bureau/SC-DBSCAN/data"
//    val dsl = ssc.textFileStream(input)

//---------------
    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()


//---------------


//    dsl.print()
//
//    dsl.foreachRDD(x => x.foreach(y => print(y)))

    ssc.start()

    ssc.awaitTermination()
  }


}
