name := "sparksqltest"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.1.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.1.0"

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector" % "1.1.0"