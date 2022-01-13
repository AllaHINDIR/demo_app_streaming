import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.streaming.Trigger


object StreamContinu {

  def main(args: Array[String]): Unit = {

    // la configuration de system avant de creer le context (voir les trois piles)
    System.setProperty("hadoop.home.dir", "D:\\hadoop");
/*
    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("SparkTest2")
      .getOrCreate()


    import spark.implicits._

    // readStream from Kafka
    val inputDF = spark
      .readStream
      .format("kafka") // org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
      .option("kafka.bootstrap.servers", "10.0.2.2:9092") // localhost:9092 = la source des données (kafka_servers)
      .option("subscribe", "test")
      .load()
      .show()

    // only select 'value' from the table,
    // convert from bytes to string
    //val rawDF = inputDF.selectExpr("CAST(value AS STRING)").as[String]


/*
    rawDF.writeStream
      .format("console")
      .outputMode("append")
      //.trigger(Trigger.ProcessingTime("2 seconds"))
      .start()
      .awaitTermination()
*/
/*
      rawDF.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic","topic_text")
        .option("checkpointLocation", "C:\\Users\\allah\\Desktop")
        .start()
        .awaitTermination()

 */
/*
    inputDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("console")
      .option("checkpointLocation", "C:\\Users\\allah\\Desktop")
      .start()

     spark.streams.awaitAnyTermination()
*/

 */


    val spark:SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      //.option("subscribePattern", "topic.*")
      //.option("startingOffsets", "earliest") // Other possible values assign and latest
      .load()


    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


    val groupCount = df.select(explode(functions.split(df("value")," ")).alias("word"))
      .groupBy("word").count()

    groupCount.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

    spark.stop()
  }

}
