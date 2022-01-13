name := "demo_app_streaming"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0"


// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.1.0"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "4.0.0"