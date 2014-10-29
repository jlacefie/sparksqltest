spark sql test
=================

The code in this repo, and the instructions below, provide examples of how to work with the DataStax Enterprise 4.6 Release of Spark SQL in a scala environment.  The README also contains a few examples showing how to work with ad hoc queries and the Spark REPL.

This project is based on the examples provided in the spark-driver-demo found here:  https://github.com/DataStaxCodeSamples/spark-driver-demo

It uses a set of csv files containing historical MLB statistics, loads them via the cqlsh COPY command, and then demonstrates the use of Spark SQL found in 4.6 DataStax Enterprise. 

# Set Up

The first step in this setup is to install DataStax Enterprise 4.6 on a cluster.  Please see the DataStax documentation for instructions on installing DataStax Enterprise.

The next step is to get the demo application up and running on a DataStax Enterprise (DSE) node.  

This demo is for example purposes only and assumes you would run this demo on a non Production node.

# Dependencies

The first dependency for this project is to ensure you have sbt 0.13.5 installed on the machine that will be used to build the demo.  As the instructions to install sbt are platform dependant, it is left up to the user to preform this step accurately.

The next dependency in this process is to execute the following shell script that will download and build a pre-released version of the spark-cassandra-connector. 
```
/resources/sst_dependency.sh
```

Once the spark-cassandra-connector has been built, it is required to be placed on each DSE node in the cluster.  Please replace the existing /usr/share/dse/spark/lib/spark-cassandra-connector_2.10-1.1.0-alpha3.jar with the newly built jar found in the <> directory.

# Demo Project Build
The next step is to setup the project in a new directory, not in the spark-cassandra-connector directory.

Execute the following commands to build the demo project.  Executing these commands will give you a feel for working with scala, particularly the sbt command.
```
  git clone https://github.com/jlacefie/sparksqltest.git

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
  arg[0] --class the class that is used to execute the demo
  arg[1] the location of the jar file we built in the last step
  arg[3] the spark master ip address which is obtained dynamically from the dsetool utility
```
  dse spark-submit --class com.sparksqltest.SparkSqlDemo target/scala-2.10/sparksqltest_2.10.jar 'dsetool sparkmaster'
  
```
# Ad hoc Querying with DSE Spark SQL and the Spark REPL
In addition to executing Spark a Stand Alone application written in Scala, we also have the ability with DSE 4.6, to execute Ad hoc queries using Spark SQL through the Spark REPL.

The following section provides a couple of sample queries that show some of the functionality of the Spark SQL library.

First we start the DSE Spark REPL and select the keyspace that contains our target tables.
```
dse spark

setKeyspace("cassandra_spark_mlbdata")

```

Now we execute queries and print results.

```
val test = sql("SELECT yearid, stint, teamid, playerid, SUM(ab), AVG(bb), SUM(g), SUM(h)/SUM(ab) from batting WHERE playerid = 'pruethu01' GROUP BY yearid, stint, teamid, playerid ORDER BY yearid, stint, teamid")

test.take(100).foreach(println)

var test = sql("SELECT DISTINCT X.playerid, Z.yearid, Z.awardid FROM (SELECT playerid, awardid, yearid FROM awardsplayers) X INNER JOIN (SELECT playerid, awardid, yearid FROM awardsplayers) Z ON X.playerid = Z.playerid AND X.awardid != Z.awardid ORDER BY X.playerid, Z.yearid, Z.awardid")

test.take(100).foreach(println)

```