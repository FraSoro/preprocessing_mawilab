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
conf.set('spark.scheduler.mode', 'FAIR')
#conf.set("spark.executor.heartbeatInterval","3600s")
sc = SparkContext(conf=conf)

print("done with startup")

from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext, HiveContext, DataFrameWriter
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import math
import Queue
import subprocess
import threading
from datetime import datetime
from pyspark.sql import functions as F
from operator import add

def entropy(assoc, data_size):
    data_size = float(data_size)
    # Reduce the association information to (value, count) pairs
    assign = assoc.map(lambda parti: (parti, 1)).reduceByKey(add)
   
    # Compute the distribution
    dist = assign.map(lambda (x, y): y/data_size)
   
    # Compute the entropy of the distribution
    entropy = dist.map(lambda u: -u*math.log(u,2)).reduce(add)
   
    entropy = entropy/math.log(assign.count(),2)
    return entropy
	
def most_used(assoc, data_size):
    data_size = float(data_size)
   
    # Reduce the association information to (value, count) pairs
    assign = assoc.map(lambda parti: (parti, 1)).reduceByKey(add)
   
  
    # Compute the distribution
    dist = assign.map(lambda (x, y): (y/data_size, x)).max()
    
    return dist
	
def get_total(assoc, data_size):
    data_size = float(data_size)
    # Reduce the association information to (value, count) pairs
    assign = assoc.map(lambda parti: (parti.ip_proto, 1)).reduceByKey(add)
    return assign.collect()
	
def get_fraction(assoc, num_TCP, column):
    
    # Reduce the association information to (value, count) pairs
    #assign = assoc.map(lambda parti : parti[column]).reduce(add)
    assign = assoc.map(lambda parti : parti[column]).filter(lambda x: x == 1)
    
    #print(assign.collect(), column)

    return assign.sum()/num_TCP

def map_IPs_to_index(x):
    print(x)
    splits = x.split('.') #split IP address in its components
    res = float(splits[0])*math.pow(256,3)+float(splits[1])*math.pow(256,2)+float(splits[2])*256+float(splits[3])
    #print(res)
    
    return res

	
def get_fraction_ips(assoc, data_size):
    data_size = float(data_size)
    # Reduce the association information to (value, count) pairs
    assign = assoc.map(lambda parti: (parti, 1)).reduceByKey(add)
    
    # Compute the distribution
    dist = assign.map(lambda (x, y): (x.ip_version,y/data_size))
  
    
    return dist.collectAsMap()

def task_sum(col, col_name):
    start = datetime.now()
    res = col.rdd.map(lambda x: x[col_name]).sum()
    key = "vol"
    dict_to_append[key] = res
    #print("sum "+col_name, col.rdd.map(lambda x: x[col_name]).sum())
    end = datetime.now()
    
    #print("sum "+col_name, str(end-start))
	
	
def task_get_min(col, col_name):
    start = datetime.now()
    key = "min_"+col_name
    dict_to_append[key] = col.rdd.map(lambda x: x[col_name]).min()
    end = datetime.now()
    #print("min "+col_name, str(end-start))

def task_get_max(col, col_name):
    start = datetime.now()
    key = "max_"+col_name
    dict_to_append[key] = col.rdd.map(lambda x: x[col_name]).max()
    end = datetime.now()
    #print("max "+col_name, str(end-start))

def task_get_avg(col, col_name):
    start = datetime.now()
    key = "avg_"+col_name
    dict_to_append[key] = col.rdd.map(lambda x: x[col_name]).mean()
    end = datetime.now()
    #print("avg "+col_name, str(end-start))
	
def task_get_var(col, col_name):
    start = datetime.now()
    key = "var_"+col_name
    dict_to_append[key] = col.rdd.map(lambda x: x[col_name]).variance()
    end = datetime.now()
    #print("var "+col_name, str(end-start))


def task_get_stdev(col, col_name):
    start = datetime.now()
    key = "stdev_"+col_name
    dict_to_append[key] = col.rdd.map(lambda x: x[col_name]).stdev()
    end = datetime.now()
    #print("stdev "+col_name, str(end-start))

	
def task_get_percentiles(col, col_name):
    start = datetime.now()
    key = "percentiles_"+col_name
    
    percentiles = [0.01, 0.02, 0.05, 0.10, 0.15, 0.20, 0.25, 0.50, 0.75, 0.90, 0.95, 0.97, 0.99]
    res = col.approxQuantile(col_name, percentiles, 0)
    res = map(str, res)
    res = ", ".join(res)
    
    dict_to_append[key] = "array("+res+")"
    end = datetime.now()
    #print("percentiles "+col_name, str(end-start))
    #print("start "+str(datetime.now()))      

def task_get_entropy(colrdd, count, name):
    start = datetime.now()
    key = "entropy_"+name
    
    res = entropy(col.rdd, col.count())
    
    dict_to_append[key] = res
    end = datetime.now()
    #print("entropy "+name, str(end-start))
    
def task_countunique(col, column_name):
    start = datetime.now()
    key = "num_"+column_name
    result = col.distinct().count()
    dict_to_append[key] = result
    end = datetime.now()
    #print("unique "+column_name, str(end-start))

def task_mu(col, column_name):
    start = datetime.now()
    mu = most_used(col.rdd, col.count())
    end = datetime.now()
    key = "most_used_"+column_name
    key1 = "frac_most_used_"+column_name
    
    dict_to_append[key] = mu[1][column_name]
    dict_to_append[key1] = mu[0]
    
    #print("most used "+column_name, str(end-start))

def task_fraction(df, column, tcptot):
    start = datetime.now()
    frac = get_fraction(df.rdd, tcptot, column)
    key = "frac_"+column
    dict_to_append[key] = frac
    end = datetime.now()
    #print("fraction "+column, str(end-start), frac)

def task_fraction_ip(datadf):
    start = datetime.now()
    frac = datadf.select("ip_version")
    frac = frac.withColumn("ip_version",F.explode(F.split('ip_version',',')))
    #frac = frac.withColumn("ip_version", frac["ip_version"].cast(DoubleType())).dropna(how="any")
    resfr = get_fraction_ips(frac.rdd, frac.count()) 
    end = datetime.now()
    dict_to_append["frac_ipv4"] = resfr[str(4)]
    dict_to_append["frac_ipv6"] = resfr[str(6)]
    
    #print("ip_version_frac", str(end-start), resfr[str(6)], resfr[str(4)])

import json
import time

    
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("ThreadingFeatureExtraction")\
        .getOrCreate()
    
    
    sqlContext = HiveContext(sc)
    #que = Queue.Queue()
    df_list = sqlContext.sql("show tables from mawi_traces").rdd.map(lambda x: x.tableName).sortBy(lambda x: x).collect()
    schema_df = sqlContext.sql("select * from mawi_schema.schema_ex").schema
    
    names = []

    for namedf in schema_df:
        names.append(namedf.name)
    
   
    
    
    get_dist = ["frame_len","ip_proto","ip_len", "ip_ttl", "tcp_dstport", "tcp_srcport", "tcp_len","tcp_winsize","udp_dstport","udp_srcport", "tcp_flags"]
    countunique = ["ip_proto", "tcp_dstport", "tcp_srcport", "udp_dstport", "udp_srcport"]
    mostused = ["tcp_dstport", "tcp_srcport", "udp_dstport", "udp_srcport"]
    tcpfraction = ["tcp_flags_ack", "tcp_flags_cwr", "tcp_flags_fin", "tcp_flags_ecn", "tcp_flags_ns", "tcp_flags_push", "tcp_flags_syn", "tcp_flags_urg"]
    
    for table_name in df_list:
        start = datetime.now()
        datadf = sqlContext.sql("select * from mawi_traces."+table_name)
        end = datetime.now()
        dict_to_append = {}
        #print("dbaccess ", str(end-start)) 

        for name in get_dist:
            col = datadf.select(name)
            col = col.withColumn(name,F.explode(F.split(name,',')))


            if name == "tcp_flags":
                col = col.rdd.filter(lambda x: x.tcp_flags != "").map(lambda x: int(x.tcp_flags, 16))
                dict_to_append["num_tcp_flags"] = col.distinct().count()
                row = Row("tcp_flags")
                col = col.map(row).toDF()
            else:
                col = col.withColumn(name, col[name].cast(DoubleType())).dropna(how="any")
                if name == "frame_len":
                    t1a = threading.Thread(target=task_sum, args=(col, name, ))
                    t1a.start()

            t = threading.Thread(target=task_get_min, args=(col, name, ))
            t.start()
            t1 = threading.Thread(target=task_get_max, args=(col, name, ))
            t1.start()
            t2 = threading.Thread(target=task_get_avg, args=(col, name, ))
            t2.start()
            t3 = threading.Thread(target=task_get_var, args=(col, name, ))
            t3.start()
            t4 = threading.Thread(target=task_get_stdev, args=(col, name, ))
            t4.start()
            t5 = threading.Thread(target=task_get_percentiles, args=(col, name, ))
            t5.start()
            t6 = threading.Thread(target=task_get_entropy, args=(col.rdd, col.count(), name, ))
            t6.start()

        for name in countunique:
            col = datadf.select(name)
            col = col.withColumn(name,F.explode(F.split(name,',')))
            col = col.withColumn(name, col[name].cast(DoubleType())).dropna(how="any")

            t = threading.Thread(target=task_countunique, args=(col, name, ))
            t.start()

            if name != "ip_proto":
                t1 = threading.Thread(target=task_mu, args=(col, name, ))
                t1.start()

    
        t3a = threading.Thread(target=task_fraction_ip, args=(datadf, ))
        t3a.start()


        start_nt = datetime.now()
        ipproto = datadf.select('ip_proto')
        ipproto = ipproto.withColumn('ip_proto',F.explode(F.split('ip_proto',',')))
        ipproto = ipproto.withColumn("ip_proto", ipproto["ip_proto"].cast(DoubleType())).dropna(how="any")
        flen = datadf.select('frame_len')
        flen = flen.withColumn("frame_len", flen["frame_len"].cast(DoubleType())).dropna(how="any")
        num_pkts = flen.count()
        dict_to_append["num_pkts"] = num_pkts
        #res = dict(get_total(ipproto.rdd, num_pkts))

        rddip = ipproto.rdd.map(lambda x: (x.ip_proto,1)).reduceByKey(add)
        rddipfr = rddip.map(lambda (x, y): (x, float(y)/num_pkts))
        res = rddip.collectAsMap()
        #print(res)
        resfr = rddipfr.collectAsMap()
        
        num_TCP_pkts = res[6.0]
        
        dict_to_append["num_icmp_pkts"] = res[1.0]
        dict_to_append["num_tcp_pkts"] = res[6.0]
        dict_to_append["num_udp_pkts"] = res[17.0]
        dict_to_append["num_gre_pkts"] = res[47.0]

        dict_to_append["frac_icmp_pkts"] = resfr[1.0]
        dict_to_append["frac_tcp_pkts"] = resfr[6.0]
        dict_to_append["frac_udp_pkts"] = resfr[17.0]
        dict_to_append["frac_gre_pkts"] = resfr[47.0]

       # print(num_ICMP_pkts, num_TCP_pkts, num_UDP_pkts, num_GRE_pkts, frac_ICMP_pkts, frac_TCP_pkts, frac_UDP_pkts, frac_GRE_pkts, frac_ICMP_pkts+frac_TCP_pkts+frac_UDP_pkts+frac_GRE_pkts)
        end_nt = datetime.now()

        #print("pkt type", str(end_nt-start_nt))



        for name in tcpfraction:
            col = datadf.select(name)
            col = col.withColumn(name,F.explode(F.split(name,',')))
            col = col.withColumn(name, col[name].cast(DoubleType())).dropna(how="any")

            t = threading.Thread(target=task_fraction, args=(col, name, num_TCP_pkts))
            t.start()
      
        start_ipsrc = datetime.now()
        ipproto = datadf.select('ip_src')
        ipproto = ipproto.withColumn('ip_src',F.explode(F.split('ip_src',','))).dropna(how="any")
        dict_to_append["num_ip_src"] = ipproto.distinct().count() #stampa
        indexed = ipproto.rdd.filter(lambda x: x.ip_src != "").map(lambda x: map_IPs_to_index(x.ip_src)) # access ip_src before splitting! the .rdd operation on dataframe returns a Row element 
        most_used_src = most_used(indexed, ipproto.count())
        
        dict_to_append["most_used_ip_src"] = most_used_src[1]
        dict_to_append["frac_most_used_ip_src"] = most_used_src[0]
        
        end_ipsrc = datetime.now()
        #print("ipsrc", str(end_ipsrc-start_ipsrc))
        
        start_ipdst = datetime.now()
        ipproto = datadf.select('ip_dst')
        ipproto = ipproto.withColumn('ip_dst',F.explode(F.split('ip_dst',','))).dropna(how="any")
        dict_to_append["num_ip_dst"] = ipproto.distinct().count() #stampa
        indexed = ipproto.rdd.filter(lambda x: x.ip_dst != "").map(lambda x: map_IPs_to_index(x.ip_dst)) # access ip_src before splitting! the .rdd operation on dataframe returns a Row element 

        most_used_dst = most_used(indexed, indexed.count())
        dict_to_append["most_used_ip_dst"] = most_used_dst[1]
        dict_to_append["frac_most_used_ip_dst"] = most_used_dst[0]
        
        
        
        #print(most_used_dst)
        end_ipdst = datetime.now()
        #print("ipdst", str(end_ipdst-start_ipdst))
        
        split = table_name.split("a")
        res = split[1]
        tablenamef = res[:8]
        
        query = "insert into table mawi_features.a"+tablenamef+"a values ("
        #query = "insert into table mawi_features.prova values ("
        
        first = True
        
        for n in names:
            if first:
                query = query+" "+str(dict_to_append[n])
                first = False
            else:
                query = query+", "+str(dict_to_append[n])
            
        query = query+")"
        
        query_track = "insert into table copied_tables.keep_track values ('"+str(table_name)+"')"
        
        print(query)
        
        try:
            sqlContext.sql(query)
            sqlContext.sql(query_track)
            print(table_name)
        except:
            sqlContext.sql("insert into table copied_tables.keep_track_wrong values ('"+str(table_name)+"')")
            print(table_name+" not correctly copied\n")