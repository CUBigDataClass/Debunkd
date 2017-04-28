from __future__ import print_function

import sys,json
from enum import Enum
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
              .config("spark.cassandra.connection.host", "ec2-35-163-127-184.us-west-2.compute.amazonaws.com")\
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
    #parsed.pprint()
    #parsed.foreach(println)

    #Month = Enum('Month', 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec')
    Month = {'Jan':'01', 'Feb':'02', 'Mar':'03', 'Apr':'04', 'May':'05',\
             'Jun':'06', 'Jul':'07', 'Aug':'08', 'Sept':'09', 'Oct':'10', 'Nov':'11', 'Dec':'12'}

    def getTweetDate(data):
        try:
            return str(datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S %z %Y'))[:10]
        except:
            return

    def getCountryCode(data):
        try:
            return data['user']['derived']['locations'][0]['country_code']
        except:
            return

    def getGeoCoordinates(data):
        try:
            x,y =  data['user']['derived']['locations'][0]['geo']['coordinates']
            return " ".join([str(x), str(y)])
        except:
            return " ".join(['0', '0'])

    def getHashTags(data):
        try:
            return  ",".join([ii for ii in data['entities']['hashtags']])
            # return "a, b"
        except:
            return ""

    def getLocation(data):
        #states = {"AL":"Alabama", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        #  "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        #  "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        #  "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        #  "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
        us_state_abbrev = { 'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY', }
        try:
            return us_state_abbrev[data['user']['derived']['locations'][0]['region']]
        except:
            return ""

    def getOriginalid(data):
        try:
            return data['retweeted_status']['id_str']
        except:
            return

    def getRetweetCount(data):
        try:
            return data['retweet_count']
        except:
            return

    #def getStatusURL(data):
    #    try:
    #        return data['object']['link']
    #    except:
    #        return

    def getURLlist(data):
        try:
            return data['entities']['urls']
        except:
            return
    def getTweetType(data):
        if 'retweeted_status' in data.keys() and type(data["retweeted_status"]) == dict:
            return "retweet"
        else :
            return "post"

    def process(rdd):
            # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())
        schemaString = "tweet_id body country_code geo_coordinates hashtags location original_status_url posted_time retweet_count status_url topic type url_list user_id"
        fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
        schema = StructType(fields)

        rowRdd = rdd.map(lambda tweet: Row(tweet_id=tweet['id_str'],\
                                           body=tweet['text'],\
                                           country_code=getCountryCode(tweet),\
                                           geo_coordinates=getGeoCoordinates(tweet),\
                                           hashtags=getHashTags(tweet),\
                                           location=getLocation(tweet),\
                                           original_status_url= getOriginalid(tweet), \
                                           posted_time=getTweetDate(tweet), \
                                           retweet_count=getRetweetCount(tweet),\
                                           status_url=None,\
                                           topic=tweet['topic'],\
                                           type= getTweetType(tweet), \
                                           url_list=getURLlist(tweet),\
                                           user_id=tweet['user']['id_str']
                                           ))

        schemaPeople = spark.createDataFrame(rowRdd, schema)
        schemaPeople.show()
        schemaPeople.write\
            .format("org.apache.spark.sql.cassandra")\
            .options(table="tweets_master", keyspace="swashbucklers")\
            .save(mode="append")

    parsed.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
