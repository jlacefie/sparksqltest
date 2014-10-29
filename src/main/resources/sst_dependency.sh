#!/bin/bash

# clone the git repository for the b1.1 branch
git clone -b b1.1 https://github.com/datastax/spark-cassandra-connector.git

# use sbt to assemble the project
cd spark-cassandra-connector
sbt assembly

# publish the project locally to prep the building of the demo
sbt publish-local