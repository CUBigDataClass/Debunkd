from __future__ import print_function

import sys,json
from enum import Enum
from fuzzywuzzy import fuzz
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *

import logging
logger = logging.getLogger("broadcast")


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
    ssc = StreamingContext(sc, 2)

    kvs = KafkaUtils.createStream(ssc, brokers, "spark-streaming-consumer", {topic: 2})
    parsed = kvs.map(lambda x: json.loads(x[1]))
    #parsed.foreachRDD(println)
    parsed.pprint()
    #parsed.foreach(println)

    #Month = Enum('Month', 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec')
    Month = {'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04', 'May':'05',\
             'Jun':'06', 'Jul':'07', 'Aug':'08', 'Sept':'09', 'Oct':'10', 'Nov':'11', 'Dec':'12'}

    def getTweetDate(data):
        try:
            return data["postedTime"][:10]
        except:
            return

    def getCountryCode(data):
        try:
            return data['gnip']['profileLocations'][0]['address']['countryCode']
        except:
            return

    def getGeoCoordinates(data):
        try:
            x,y = data['gnip']['profileLocations'][0]['geo']['coordinates']
            return " ".join([str(x), str(y)])

        except:
            return " ".join(['0', '0'])

    def getHashTags(data):
        try:
            return  ",".join([ii['text'] for ii in data['twitter_entities']['hashtags']])
            # return "a, b"
        except:
            return ""

    def getLocation(data):
        states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

        try:
            return next((s for s in states if s in data['actor']['location']['displayName']), "")
        except:
            return ""


    def getOriginalStatus(data):
        try:
            return data['object']['object']['link']
        except:
            return

    def getRetweetCount(data):
        try:
            return data['retweetCount']
        except:
            return

    def getStatusURL(data):
        try:
            return data['object']['link']
        except:
            return

    def getURLlist(data):
        try:
            return data['twitter_entities']['url']
        except:
            return

    def filterRecord(rdd):
        if (fuzz.token_set_ratio("hillary sold weapons to isis", rdd['body']) > 60):
          print(rdd['body'])


    def process(rdd):
            # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())
        schemaString = "tweet_id body country_code geo_coordinates hashtags location original_status_url posted_time retweet_count status_url topic type url_list user_id"
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
        schema = StructType(fields)

        dictRdd = rdd.map(lambda tweet: dict('tweet_id': tweet['object']['id'],\
                                           'body': tweet['body'],\
                                           'country_code': getCountryCode(tweet),\
                                           'geo_coordinates': getGeoCoordinates(tweet),\
                                           'hashtags': getHashTags(tweet),\
                                           'location': getLocation(tweet),\
                                           'original_status_url':  getOriginalStatus(tweet), \
                                           'posted_time': getTweetDate(tweet), \
                                           'retweet_count': getRetweetCount(tweet),\
                                           'status_url': getStatusURL(tweet),\
                                           'topic': tweet['topic'],\
                                           'type':  tweet['verb'], \
                                           'url_list': getURLlist(tweet),\
                                           'user_id': tweet['actor']['id']
                                           )).filter(filterRecord)
        # schemaPeople = spark.createDataFrame(rowRdd, schema)
        # schemaPeople.show()
        # schemaPeople.write\
        #     .format("org.apache.spark.sql.cassandra")\
        #     .options(table="tweets_master", keyspace="swashbucklers")\
        #     .save(mode="append")

    parsed.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
