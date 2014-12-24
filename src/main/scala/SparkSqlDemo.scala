/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.SqlRowWriter
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object SparkSqlDemo {
  //set up configuration item
  val keyspace = "cassandra_spark_mlbdata"

  def main(args: Array[String]) {
	  if(args.length < 1) {
    	  println("Spark master URL missing from arguments, use `dsetool sparkmaster` to determinate the local spark master connection URI")
		  System.exit(-1)
	  }
	var sparkUri = args(0)
    val sparkconf = new SparkConf()
      .setAppName("Spark SQL Test")
      .setSparkHome(System.getenv("SPARK_HOME"))

    //create a new SparkContext
    val sc = new SparkContext(sparkUri, "test", sparkconf)
    //create a new SparkSQLContext
    val sqlc = new CassandraSQLContext(sc)

    //set the default keyspace
    sqlc.setKeyspace(keyspace)

    //prepare the demo
    CassandraConnector(sparkconf).withSessionDo { session =>
      // insure for test we are not going to look at existing data, but new data
      session.execute(s"DROP TABLE IF EXISTS $keyspace.awardssummary")
      session.execute(s"DROP TABLE IF EXISTS $keyspace.awardssummary2")
      session.execute(s"DROP TABLE IF EXISTS $keyspace.awardssummary3")
      session.execute(s"DROP TABLE IF EXISTS $keyspace.awardsplayerscollections")
      session.execute(s"DROP TABLE IF EXISTS $keyspace.awardsplayerscollections2")
      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.awardssummary (awardid text,lgid text,bats text,count int,PRIMARY KEY (awardid, lgid, bats))")
      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.awardssummary2 (awardid text,lgid text,bats text,count int,PRIMARY KEY (awardid, lgid, bats))")
      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.awardssummary3 (awardid text,lgid text,bats text,count int,PRIMARY KEY (awardid, lgid, bats))")
      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.awardsplayerscollections (awardid text,playerset set<text>,playerlist list<text>,playermap map<text,int>,PRIMARY KEY (awardid))")
      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.awardsplayerscollections2 (awardid text,playerset set<text>,playerlist list<text>,playermap map<text,int>,PRIMARY KEY (awardid))")
    }

    //show a simple select query
    println("test select")
    simpleSelect(sqlc)

    //show a simple insert statement
    println("test load via insert select")
    loadWithInsert(sqlc)

    //show a more complicated mapping insert
    println("test load via saveToCassandra")
    loadWithSave(sqlc, sc)

    //show an async insert with the datastax driver
    println("test load via async")
    loadWithAsync(sparkconf, sqlc)

    //show an async insert containing collections
    println("test loading collections")
    loadCollecsIndividually(sparkconf, sqlc)

    //show an insert with collections and rdd colleciton manipulation
    println("test loading collections")
    loadCollecsPostReduce(sqlc)

    System.exit(0)
  }

  /** Example of how to execute a Spark SQL statement against Cassandra */
  def simpleSelect(sqlc: CassandraSQLContext): Unit ={
    //create an rdd useing SparkSQL
    val test = sqlc.sql("SELECT a.playerid, a.awardid, a.yearid, a.lgid, a.notes, a.tie FROM awardsplayers a INNER JOIN master m ON a.playerid = m.playerid WHERE namelast LIKE 'T%' UNION SELECT a.playerid, a.awardid, a.yearid, a.lgid, a.notes, a.tie FROM awardsmanagers a INNER JOIN master m ON a.playerid = m.playerid WHERE namelast LIKE 'T%'")

    //print a test result set
    test.take(10).foreach(println)
  }

  /** Example of how to load data back into cassandra using a simple insert into select statement */
  def loadWithInsert(sqlc: CassandraSQLContext): Unit ={
    //remove nulls to avoid issues inserting back into Cassandra
    val source = sqlc.sql("INSERT INTO awardssummary SELECT UPPER(ap.awardid), ap.lgid, IF(m.bats is null, '', m.bats), count(*) FROM awardsplayers ap INNER JOIN master m ON ap.playerid = m.playerid WHERE ap.yearid > 1940 GROUP BY ap.awardid, ap.lgid, m.bats ORDER BY ap.awardid, ap.lgid, m.bats")

    //test that the save was successful
    val test = sqlc.sql("SELECT * FROM awardssummary")
    test.take(10).foreach(println)

  }

  /** Example of how to preform RDD operations on the results of a Spark SQL query and then save the final Rdd back to Cassandra */
  def loadWithSave(sqlc: CassandraSQLContext, sc: SparkContext): Unit ={

    //step 1 - find a count of awards by award, league, and batting position from 1940 to today and Capitalize the Award Name, remove nulls
    //nulls should be removed per here - https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Row.scala
    val source = sqlc.sql("SELECT UPPER(ap.awardid), ap.lgid, IF(m.bats is null, '', m.bats), count(*) FROM awardsplayers ap INNER JOIN master m ON ap.playerid = m.playerid WHERE ap.yearid > 1940 GROUP BY ap.awardid, ap.lgid, m.bats ORDER BY ap.awardid, ap.lgid, m.bats")

    //other manipulations could go here

    //step 2 - save to cassandra
    source.saveToCassandra(keyspace, "awardssummary2",SomeColumns("awardid","lgid","bats","count"))(CassandraConnector(sc.getConf), SqlRowWriter.Factory)

    //test that the save was successful
    val test = sqlc.sql("SELECT * FROM awardssummary2")
    test.take(10).foreach(println)

  }

  /** Example of how to load data back into cassandra using the DataStax Java Driver and asychronous session execution statements */
  def loadWithAsync(sparkConf: SparkConf, sqlc: CassandraSQLContext): Unit = {
    //connector used for cluster connections
    val connector = CassandraConnector(sparkConf)

    //just like other insert steps
    val source = sqlc.sql("SELECT UPPER(ap.awardid), ap.lgid, IF(m.bats is null, '', m.bats), count(*) FROM awardsplayers ap INNER JOIN master m ON ap.playerid = m.playerid WHERE ap.yearid > 1940 GROUP BY ap.awardid, ap.lgid, m.bats ORDER BY ap.awardid, ap.lgid, m.bats")

    //map each row and preform the insert statement for every row
    source.map(row => {
       connector.withSessionDo(session => {
         // prepare the statement once per executor
         val insertStatement = session.prepare(s"insert into $keyspace.awardssummary3 ( awardid, lgid, bats, count) values (?, ?, ?, ?)")

         session.executeAsync(
          insertStatement.bind(row.getString(0), row.getString(1), row.getString(2), int2Integer(row.getLong(3).toInt)))
      })
    }).foreach(x => x.getUninterruptibly())

    //test that the save was successful
    val test = sqlc.sql("SELECT * FROM awardssummary3")
    test.take(10).foreach(println)
  }

  /** Example of how to load collection data into cassandra using the DataStax Java Driver and asychronous session execution statements */
  // same process as loadWithAsync
  def loadCollecsIndividually(sparkConf: SparkConf, sqlc: CassandraSQLContext): Unit = {

    val connector = CassandraConnector(sparkConf)

    val source = sqlc.sql("SELECT DISTINCT awardid, playerid, count(*) from awardsplayers GROUP BY awardid, playerid")

    source.map(row => {
      // create java.Util.Set for the DataStax Java Driver, Scala data types aren't accepted by the driver
      val s: java.util.Set[String] = Set(row.getString(1))
      val l: java.util.List[String] = List(row.getString(1))
      val m: java.util.Map[String, Int] = Map(row.getString(1) -> row.getLong(2).toInt)
      connector.withSessionDo(session => {
        val insertStatement = session.prepare(s"UPDATE $keyspace.awardsplayerscollections SET playerset = playerset + ?, playerlist = playerlist + ?, playermap = playermap + ? WHERE awardid = ?")
        session.executeAsync(
          insertStatement.bind(s,l, m, row.getString(0)))
      })
    }).foreach(x => x.getUninterruptibly())

    val test = sqlc.sql("SELECT * FROM awardsplayerscollections")
    test.take(10).foreach(println)
  }

  /** Example of how to preform RDD operations  on the results of a Spark SQL query and then save the final Rdd back to Cassandra with collections*/
  def loadCollecsPostReduce(sqlc: CassandraSQLContext): Unit = {

    val source = sqlc.sql("SELECT DISTINCT awardid, playerid, count(*) from awardsplayers GROUP BY awardid, playerid")

    // needed to explicitly define a key value pair RDD to manage the 3 different collections
    // leveraged a collector object for aggregation
    val target : RDD[(String, AwardsCollection)] = source.map(row =>
      (row.getString(0),new AwardsCollection(row.getString(0),Set(row.getString(1)),List(row.getString(1)),Map(row.getString(1) -> row.getLong(2))))
    //reduce by key to combine set values
    ).reduceByKey(_ + _)

    // mapped output of key value RDD to a new tuple based RDD to prep for the save to Cassandra
    target.map((x: (String ,AwardsCollection)) => (x._2.awardid, x._2.playerSet, x._2.playerList, x._2.playerMap))
      .saveToCassandra(keyspace, "awardsplayerscollections2",SomeColumns("awardid","playerset","playerlist","playermap"))

    //test that the save was successful
    val test = sqlc.sql("SELECT * FROM awardsplayerscollections2")
    test.take(10).foreach(println)
  }
}



