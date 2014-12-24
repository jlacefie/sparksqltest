Spark SQL Example
=================

The code in this repo, and the instructions below, provide examples of how to work with the DataStax Enterprise 4.6 Release of Spark SQL in a Scala environment.  The README also contains a few examples showing how to work with Ad hoc queries and the Spark REPL.

This project is based on the examples provided in the spark-driver-demo found here:  https://github.com/DataStaxCodeSamples/spark-driver-demo

It uses a set of CSV files containing historical Major League Baseball statistics, loads them via the cqlsh COPY command, and then demonstrates the use of Spark SQL found in 4.6 DataStax Enterprise.

https://academy.datastax.com/demos/datastax-enterprise-46-and-spark-sql

# Set Up

The first step in this setup is to install DataStax Enterprise 4.6 on a cluster.  Please see the DataStax documentation for instructions on installing DataStax Enterprise.

The next step is to get the demo application up and running on a DataStax Enterprise (DSE) node.  

This demo is for example purposes only and assumes you would run this demo on a non Production node.

# Dependencies

 * [sbt 0.13.5](http://www.scala-sbt.org/) or above.
 * [spark-cassandra-connector 1.1.0](https://github.com/datastax/spark-cassandra-connector) and above(already included in DSE 4.6)

# Demo Project Build
The next step is to setup the project. To start go back to the sparksqltest directory.

Execute the following commands to build the demo project.  Executing these commands will give you a feel for working with scala, particularly the sbt command.

```bash
  cd sparksqltest/data

  wget http://seanlahman.com/files/database/lahman-csv_2014-02-14.zip

  unzip lahman-csv_2014-02-14.zip
  
  cqlsh < ../src/main/resources/cql/schema.cql 

  cqlsh < ../src/main/resources/cql/load_data.cql
  
  cd ..

  sbt package

```

# Execute the Demo

The final step is to execute the sparksqltest jar on the DSE node from the sparksqltest directory created in the last step. 

This command uses the dse spark-submit operation and passes in the following arguments:

   * __arg[0]__: --class the class that is used to execute the demo
   * __arg[1]__: the location of the jar file we built in the last step
   * __arg[3]__: the spark master ip address which is obtained dynamically from the dsetool utility
   
```bash
  dse spark-submit --class SparkSqlDemo target/scala-2.10/sparksqltest_2.10-1.0.jar `dsetool sparkmaster`
  
```
# Ad hoc Querying with DSE Spark SQL and the Spark REPL
In addition to executing Spark as a Stand Alone application written in Scala, we also have the ability with DSE 4.6, to execute Ad hoc queries using Spark SQL through the Spark REPL.

The following section provides a couple of sample queries that show some of the functionality of the Spark SQL library.

First we start the DSE Spark REPL and select the keyspace that contains our target tables.
```bash
dse spark
```

Switch to keyspace:

```scala
setKeyspace("cassandra_spark_mlbdata")

```

Now we execute queries and print results.

```scala
val test = sql("SELECT yearid, stint, teamid, playerid, SUM(ab), AVG(bb), SUM(g), SUM(h)/SUM(ab) from batting WHERE playerid = 'pruethu01' GROUP BY yearid, stint, teamid, playerid ORDER BY yearid, stint, teamid")

test.take(100).foreach(println)

var test = sql("SELECT DISTINCT X.playerid, Z.yearid, Z.awardid FROM (SELECT playerid, awardid, yearid FROM awardsplayers) X INNER JOIN (SELECT playerid, awardid, yearid FROM awardsplayers) Z ON X.playerid = Z.playerid AND X.awardid != Z.awardid ORDER BY X.playerid, Z.yearid, Z.awardid")

test.take(100).foreach(println)

```