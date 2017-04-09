# from __future__ import print_function

import sys,json
from enum import Enum
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession

class Month(Enum):
    Jan='01'
    Feb='02'
    Mar='03'
    Apr='04'
    May='05'
    Jun='06'
    Jul='07'
    Aug='08'
    Sep='09'
    Oct='10'
    Nov='11'
    Dec='12'

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder\
              .appName("SparkCassandraApp")\
              .config("spark.cassandra.connection.host", "172.32.13.183")\
              .config("spark.cassandra.connection.port", "9042")\
              .master("local[2]")\
              .getOrCreate();
    return globals()['sparkSessionSingletonInstance']


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    brokers, topic = sys.argv[1:]
    sc = SparkContext(appName="SparkCassandraApp")
    ssc = StreamingContext(sc, 1)

    kvs = KafkaUtils.createStream(ssc, brokers, "spark-streaming-consumer", {topic: 1})
    parsed = kvs.map(lambda x: json.loads(x[1]))

    def process(rdd):
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            rowRdd = rdd.map(lambda tweet: Row(id=tweet['id_str']))
            tweetsDataFrame = spark.createDataFrame(rowRdd)

            tweetsDataFrame.write\
                .format("org.apache.spark.sql.cassandra")\
                .options(table="test_tweets", keyspace="swashbucklers")\
                .save(mode="append")
            
            # Creates a temporary view using the DataFrame.
            #tweetsDataFrame.createOrReplaceTempView("tweets")

            # Do word count on table using SQL and print it
            #tweetidsDataFrame = \
            #    spark.sql("select id from test_tweets")
            #tweetidsDataFrame.show()

        except:
            pass

    parsed.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
