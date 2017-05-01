##the script generates aggrregate count for various topics over state
##input is the data in cassandra table swashbucklers.tweets_master
##updates cassandra tables swashbucklers.topic_state_aggregates

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import count



if __name__ == "__main__":
    ##Create a spark session with cassandra
    spark = SparkSession.builder.appName("SparkCassandraApp").config("spark.cassandra.connection.host", "sb-k8s-ingress-1525602578.us-west-2.elb.amazonaws.com").config("spark.cassandra.connection.port", "9042").master("local[2]").getOrCreate();

    ##Load the table as a dataframe
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table="tweets_master", keyspace="swashbucklers").load();
    ##df.show()

    ##aggregate over various states for a given topic
    topics_state_count = df.select("topic","location").groupBy("topic", "location").agg(count("location").alias("count")).na.drop()
    topics_state_count.write.format("org.apache.spark.sql.cassandra").options(table="state_aggregates", keyspace="swashbucklers").save(mode="overwrite")
    topics_state_count.unpersist()
    ##topics_count.show()
