#!/bin/bash

# clone the git repository for the b1.1 branch
echo "git clone -b b1.1 https://github.com/datastax/spark-cassandra-connector.git"
git clone -b b1.1 https://github.com/datastax/spark-cassandra-connector.git

# use sbt to assemble the project
"echo sbt assembly"
cd spark-cassandra-connector
sbt assembly

# publish the project locally to prep the building of the demo
echo "sbt publish-local"
sbt publish-local

echo "success"