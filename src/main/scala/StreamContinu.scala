import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object StreamContinu {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("SparkTest")
      .getOrCreate()

    import spark.implicits._

    // readStream from Kafka
    val inputDF = spark
      .readStream
      .format("kafka") // org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
      .option("kafka.bootstrap.servers", "localhost:9092") // localhost:9092 = la source des données (kafka_servers)
      .option("subscribe", "topic1")
      .load()

    // only select 'value' from the table,
    // convert from bytes to string
    val rawDF = inputDF.selectExpr("CAST(value AS STRING)").as[String]

    rawDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()
    
  }

}
