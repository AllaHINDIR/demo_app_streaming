import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import com.datastax.oss.driver.api.core.uuid.Uuids

import java.util.Date // com.datastax.cassandra:cassandra-driver-core:4.0.0


object KafkaToSparkToCassendra {

  //create an object immutable
  case class overview(vol: String, registered_at: Double, total: Double , in_flight:Int)
  case class country(country: String, country_id: Double, country_name: String, registered_at: Date, origine: Int, over:Int )

  def main(args: Array[String]): Unit = {

    // initialize Spark
    // nous avons configuré notre systeme tel que l'allocation se fait d'une maniere automatique selon le processing et la memoire !
    val spark = SparkSession
      .builder
      .master("local[1]") // l'adress du cluster
      .appName("Stream Handler")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.driver.cores",1)
      .config("spark.driver.memory","1g")
      .config("spark.executor.memory","1g")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.executor.cores", 4)
      .config("spark.dynamicAllocation.minExecutors","1")
      .config("spark.dynamicAllocation.maxExecutors","5")
      .getOrCreate()

    import spark.implicits._

    // read from Kafka
    val inputDF = spark
      .readStream
      .format("kafka") // org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
      .option("kafka.bootstrap.servers", "localhost:9092") // l'adresse de cluster Kafka
      .option("subscribe", "overview")
      .load()

    // only select 'value' from the table,
    // convert from bytes to string
    val rawDF = inputDF.selectExpr("CAST(value AS STRING)").as[String]

    // split each row on comma, load it to the case class
    val expandedDF = rawDF.map(row => row.split(","))
      .map(row => overview(
        row(1),
        row(3).toDouble,
        row(4).toDouble,
        row(5).toInt,
      ))

    // groupby and aggregate
    val summaryDf = expandedDF
      .groupBy("vol")
      .agg(sum("total"))

    // create a dataset function that creates UUIDs
    val makeUUID = udf(() => Uuids.timeBased().toString)

    // add the UUIDs and renamed the columns
    // this is necessary so that the dataframe matches the
    // table schema in cassandra
    val summaryWithIDs = summaryDf.withColumn("source_id", makeUUID())
      .withColumnRenamed("registered_at", "registered_at")
      .withColumnRenamed("total", "total")
      .withColumnRenamed("in_flight", "in_flight")

    // write dataframe to Cassandra
    val query = summaryWithIDs
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
        println(s"Writing to Cassandra $batchID")
        batchDF.write
          .cassandraFormat("overview", "stuff") // table, keyspace
          .mode("append")
          .save()
      }
      .outputMode("update")
      .start()

    // until ^C
    query.awaitTermination()
  }
}
