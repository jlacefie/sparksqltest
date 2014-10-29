spark sql test
=================

The code in this repo, and the instructions below use DataStax Spark SQl to provide examples of how to work with Spark SQL in an scala environment.

This project is based on the examples provided in the spark-driver-demo found here:  https://github.com/DataStaxCodeSamples/spark-driver-demo

It uses a set of csv files containing historical MLB statistics. loads them int using the cqlsh COPY command, and then demonstrates use of spark sql from scala using DataStax Enterprise, 

# set up

install DataStax Enterprise

from a DSE node, assuming a dev node, run the following

note: since this is an eap project from DataStax, there is a branch, pre-released, version of the spark-cassandra-connector required

first get the connector and publish it locally.  ingore warnings during the assembly
be sure to use sbt 0.13.5

```
git clone -b b1.1 https://github.com/datastax/spark-cassandra-connector.git

cd spark-cassandra-connector

sbt assembly

sbt publish-local
```

now setup the project in a new directory, not in the spark-cassandra-connector directory

this assumes we have a standalone version of sbt.  
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

On DSE node in sparksqltest directory 
```
  dse spark-submit --class com.sparksqltest.SparkSqlDemo target/scala-2.10/sparkscalatest_2.10.jar 'dsetool sparkmaster'
  
```

On DSE to execute one off Spark SQL statements via the Spark REPL


```
dse spark

setKeyspace("cassandra_spark_mlbdata")

val test = sql("SELECT yearid, stint, teamid, playerid, SUM(ab), AVG(bb), SUM(g), SUM(h)/SUM(ab) from batting WHERE playerid = 'pruethu01' GROUP BY yearid, stint, teamid, playerid ORDER BY yearid, stint, teamid")

test.take(100).foreach(println)

var test = sql("SELECT DISTINCT X.playerid, Z.yearid, Z.awardid FROM (SELECT playerid, awardid, yearid FROM awardsplayers) X INNER JOIN (SELECT playerid, awardid, yearid FROM awardsplayers) Z ON X.playerid = Z.playerid AND X.awardid != Z.awardid ORDER BY X.playerid, Z.yearid, Z.awardid")

test.take(100).foreach(println)

```