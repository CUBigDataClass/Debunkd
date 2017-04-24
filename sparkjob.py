from __future__ import print_function

import sys,json
from enum import Enum
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession


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
            return data['gnip']['profileLocation'][0]['address']['countryCode']
        except:
            return

    def getGeoCoordinates(data):
        try:
            return data['gnip']['profileLocation'][0]['geo']['coordinates']
        except:
            return

    def getHashTags(data):
        try:
            return [ii['text'] for ii in data['twitter_entities']['hashtags']]
        except:
            return

    def getLocation(data):
        try:
            return data['actor']['location']['displayName']
        except:
            return

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


    def process(rdd):
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            rowRdd = rdd.map(lambda tweet: Row(tweet_id=tweet['object']['id'],\
                                               body=tweet['body'],\
                                               country_code=getCountryCode(tweet),\
                                               geo_coordinates=getGeoCoordinates(tweet),\
                                               hashtags=getHashTags(tweet),\
                                               location=getLocation(tweet),\
                                               original_status_url= getOriginalStatus(tweet), \
                                               posted_date=getTweetDate(tweet), \
                                               retweet_count=getRetweetCount(tweet),\
                                               status_url=getStatusURL(tweet),\
                                               topic=tweet['topic'],\
                                               type= tweet['verb'], \
                                               url_list=getURLlist(tweet),\
                                               user_id=tweet['actor']['id']
                                               ))

            tweetsDataFrame = spark.createDataFrame(rowRdd)

            tweetsDataFrame.write\
                .format("org.apache.spark.sql.cassandra")\
                .options(table="tweets_master", keyspace="swashbucklers")\
                .save(mode="append")

        except:
            pass

    parsed.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
