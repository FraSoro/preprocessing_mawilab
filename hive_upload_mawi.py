import os
import sys

os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda-4.1.1/bin/python"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-7-oracle-cloudera/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.0.0.cloudera2-1.cdh5.7.0.p0.118100/lib/spark2"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.3-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

os.environ['PYSPARK_SUBMIT_ARGS'] = "--master yarn pyspark-shell"

print("starting")

from pyspark import SparkConf
from pyspark import SparkContext
conf = SparkConf()
conf.setMaster('yarn-client')
conf.setAppName('anaconda-pyspark')
sc = SparkContext(conf=conf)

print("done with startup")

from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import math
from datetime import datetime
import time

import subprocess

file_dir = subprocess.Popen(["hadoop", "fs", "-ls", "/user/big-dama/mawi_traces/extracted"], stdout=subprocess.PIPE)

file_list = []

i = 0
for file_name in file_dir.stdout:
    #print(file_name)
    if i != 0:
        splits = file_name.split("/")
        file_list.append(splits[5].lstrip('\n').strip())
        #print(splits[5].lstrip('\n'))
    i = i+1

#print(file_list)


def get_table_name(x):
    split = x.tableName.split("a")
    return split[1]
	
import os

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("HiveDBCreation")\
        .getOrCreate()

    #iteration on all the content of the folder!
   
   
    sqlContext = SQLContext(sc)
    
    dblist = sqlContext.sql("show tables from mawi_traces")
    dblist = dblist.select("tableName").rdd.map(lambda x: get_table_name(x)).collect()
    
    #print(dblist.rdd.count())

    
    file_list = list(set(file_list) - set(dblist)) 
   
    for file_name in file_list:
        #file_name=file_name.lstrip('\n').strip()
        tablename = "mawi_traces.a"+str(file_name.strip())+"a"
        
        if file_name not in dblist:
            try:
                path = os.path.join("hdfs://bigdama-vworker3-phy1.bigdama.local/user/big-dama/mawi_traces/extracted/", file_name.strip())
                packetdata_rdd = sc.textFile(path)
                df = packetdata_rdd.map(lambda l: l.split('\t')).toDF(["timestamp", "frame_len", "ip_proto", "ip_len", "ip_ttl", "ip_version", "tcp_dstport", "tcp_srcport", "tcp_flags", "tcp_flags_ack", "tcp_flags_cwr", "tcp_flags_fin", "tcp_flags_ecn", "tcp_flags_ns", "tcp_flags_push", "tcp_flags_syn", "tcp_flags_urg", "tcp_len", "tcp_winsize", "udp_srcport", "udp_dstport", "ip_src", "ip_dst"])
                print(tablename)
                df.write.saveAsTable(tablename)
               
            except:
                print(tablename +" not correctly copied!")

