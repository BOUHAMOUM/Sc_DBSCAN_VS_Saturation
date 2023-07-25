package streaming

import org.apache.spark.sql.SparkSession

object SparkStreamingApp2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[2]")
      .getOrCreate()

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()


//    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts = lines

//      words.groupBy("value").count()


    val query = wordCounts.writeStream
//      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }
}
