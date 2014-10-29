spark sql test
=================

The code in this repo, and the instructions below use DataStax Spark SQl to provide examples of how to work with Spark SQL in an scala environment.

This project is based on the examples provided in the spark-driver-demo found here:  https://github.com/DataStaxCodeSamples/spark-driver-demo

It uses a set of csv files containing historical MLB statistics. loads them int using the cqlsh COPY command, and then demonstrates use of spark sql from scala using DataStax Enterprise, 

# set up

install DataStax Enterprise

from a DSE node, assuming a dev node
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
  dse spark-submit --class com.sparksqltest.SparkSqlDemo sparkscalatest_2.10-0.4-SNAPSHOT.jar 'dsetool sparkmaster'
  
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