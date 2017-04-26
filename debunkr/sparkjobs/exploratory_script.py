##the script generates aggrregate count for various topics over date
##input is the data in cassandra table swashbucklers.tweets_master
##updates cassandra tables swashbucklers.topic_date_aggregates

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import count


if __name__ == "__main__":
    ##spark session to connect to cassandra database
    spark = SparkSession.builder.appName("SparkCassandraApp").config("spark.cassandra.connection.host", "172.32.13.183").config("spark.cassandra.connection.port", "9042").master("local[2]").getOrCreate();

    ##input table as dataframe
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table="tweets_master", keyspace="swashbucklers").load();
    df.show()

    ##gives how many times a user has tweeted the topic
    topics_user_count = df.select("topic","user_id").groupBy("topic", "user_id").agg(count("topic").alias("topic_count"))
    topics_user_count.show()
    topics_user_count.count()

    ##lets get how many times the same set of users_ids have teweeted about various topics. This gives us a small subset of users who propogate/fall for/debunk fake/false news
    user_count = topics_user_count.groupBy("user_id").agg(count("user_id").alias("count")).filter("count > 3")
    user_count.show()

    ##further we can check these accounts and see what these accounts tweet,how often they tweet .. 
    topics_user_count.unpersist()
