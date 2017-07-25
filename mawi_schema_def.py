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

from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import math
import Queue
import subprocess
import threading
from datetime import datetime
from pyspark.sql import functions as F
from operator import add

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("ThreadingFeatureExtraction")\
        .getOrCreate()
    
    
    sqlContext = HiveContext(sc)
    
    #Create tabele schema to be used for feature importing

    query_string = "create table if not exists mawi_schema.schema_ex (vol DOUBLE, num_pkts INT, "
    get_dist = ["frame_len","ip_proto","ip_len", "ip_ttl", "tcp_dstport", "tcp_srcport", "tcp_len","tcp_winsize","udp_dstport","udp_srcport", "tcp_flags"]
    countunique = ["ip_proto", "tcp_dstport", "tcp_srcport", "udp_dstport", "udp_srcport", "ip_src", "ip_dst", "tcp_flags"]
    mostused = ["tcp_dstport", "tcp_srcport", "udp_dstport", "udp_srcport", "ip_src", "ip_dst"]
    tcpfraction = ["tcp_flags_ack", "tcp_flags_cwr", "tcp_flags_fin", "tcp_flags_ecn", "tcp_flags_ns", "tcp_flags_push", "tcp_flags_syn", "tcp_flags_urg"]
    pkttype = ["ICMP_pkts", "TCP_pkts", "UDP_pkts", "GRE_pkts"]

    
    i = 0
    for name in get_dist:
        if i == 0:
            query_string += "min_"+name+" double, max_"+name+" double, avg_"+name+" double, var_"+name+" double, stdev_"+name+" double, percentiles_"+name+" array<double>, entropy_"+name+" double"
        else:
            query_string += ", min_"+name+" double, max_"+name+" double, avg_"+name+" double, var_"+name+" double, stdev_"+name+" double, percentiles_"+name+" array<double>, entropy_"+name+" double"
        i = i+1
        
    query_string += ", frac_ipv4 double, frac_ipv6 double"
        
    for name in countunique:
        query_string += ", num_"+name+" int"
        
    for name in mostused:
        
        if name != "ip_src" and name != "ip_dst":
            query_string += ", most_used_"+name+" int, frac_most_used_"+name+" double"
        else:
            query_string += ", most_used_"+name+" bigint, frac_most_used_"+name+" double"
        
    for name in tcpfraction:
        query_string += ", frac_"+name+" double"
        
    for name in pkttype:
        query_string += ", num_"+name+" int, frac_"+name+" double"
    
    query_string += ")"
    
    print(query_string)
    
    sqlContext.sql(query_string)
    
    