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


def get_table_name(x):
    split = x.split("a")
    res = split[1]
    return res[:8]

file_dir = subprocess.Popen(["hadoop", "fs", "-ls", "/user/big-dama/mawi_traces/unzipped"], stdout=subprocess.PIPE)

file_list = []

i = 0
for file_name in file_dir.stdout:
    #print(file_name)
    if i > 1:
        splits = file_name.split("/")
        split1 = splits[5].split(".")
        get_sub = split1[0].lstrip('\n')
        file_list.append(get_sub[:8])
        #print(splits[5].lstrip('\n'))
    i = i+1

print(file_list)

import os

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("HiveDBCreation")\
        .getOrCreate()

    #iteration on all the content of the folder!
   
   
    sqlContext = SQLContext(sc)
    hiveCtx = HiveContext(sc)
    
    for name in file_list:
        
        statement = "CREATE TABLE IF NOT EXISTS mawi_features.a"+ name+"a like mawi_schema.schema_ex" #schema specification?
        spark.sql(statement)
        
    