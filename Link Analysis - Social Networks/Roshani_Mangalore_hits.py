#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This is an example implementation of PageRank. For more conventional use,
Please refer to PageRank implementation provided by graphx

Example Usage:
bin/spark-submit examples/src/main/python/pagerank.py data/mllib/pagerank_data.txt 10
"""
from __future__ import print_function
import re
import sys
import os
from operator import add
from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    for url in urls: yield (url, rank)

def outgoingNeighbors(urls):
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

def incomingNeighbors(urls):
    parts = re.split(r'\s+', urls)
    return parts[1], parts[0]


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations> <output-dir>", file=sys.stderr)
        exit(-1)

    print("WARN: This is a naive implementation of PageRank and is given as an example!\n" +
          "Please refer to PageRank implementation provided by graphx",
          file=sys.stderr)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    # Loads in input file. It should be in format of:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    outgoing_links = lines.map(lambda urls: outgoingNeighbors(urls)).distinct().groupByKey().cache()
    incoming_links = lines.map(lambda urls: incomingNeighbors(urls)).distinct().groupByKey().cache()
    
    hubs_nor = outgoing_links.map(lambda (url, neighbors): (url, 1.0))
    for iteration in xrange(int(sys.argv[2])):
    
        auth_contribs = outgoing_links.join(hubs_nor).flatMap(lambda (url, (urls, hub)):computeContribs(urls, hub))
        auths = auth_contribs.reduceByKey(add)    
        max_value1 = max(auths.collect(), key=lambda x:x[1])[1]
        auths_nor = auths.mapValues(lambda rank:rank/max_value1)
    
        hub_contribs = incoming_links.join(auths_nor).flatMap(lambda (url, (urls, auth)):computeContribs(urls, auth))
        hubs = hub_contribs.reduceByKey(add)
        max_value2 = max(hubs.collect(), key=lambda x:x[1])[1]
        hubs_nor = hubs.mapValues(lambda rank:rank/max_value2)
    
   
    if not os.path.exists(sys.argv[3]):
		os.makedirs(sys.argv[3])

    fp1=open(sys.argv[3]+'/authority.txt','w+')
    fp2=open(sys.argv[3]+'/hub.txt','w+')
  
    auths_nor=auths_nor.sortBy(lambda x:int(x[0]))
    hubs_nor=hubs_nor.sortBy(lambda x:int(x[0]))

    for (link, rank) in auths_nor.collect():
		fp1.write(str(link)+","+str(format(rank, '.5f'))+"\n")
	fp1.close()
	
    for (link, rank) in hubs_nor.collect():
		fp2.write(str(link)+","+str(format(rank, '.5f'))+"\n")
	fp2.close()
	
    spark.stop()


