##the script generates aggrregate count for various topics over date
##input is the data in cassandra table swashbucklers.tweets_master
##updates cassandra tables swashbucklers.topic_date_aggregates

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import count


if __name__ == "__main__":
  ##get the spark session
  spark = SparkSession.builder.appName("SparkCassandraApp").config("spark.cassandra.connection.host", "172.32.13.183").config("spark.cassandra.connection.port", "9042").master("local[2]").getOrCreate();

  ##read table as a dataframe
  df = spark.read.format("org.apache.spark.sql.cassandra").options(table="tweets_master", keyspace="swashbucklers").load();

  ##aggregate over dates for a given topic
  topics_date_count = df.select("topic","posted_time").groupBy("topic", "posted_time").agg(count("posted_time").alias("count"))

  ##update the cassandra table with the counts
  topics_date_count.write.format("org.apache.spark.sql.cassandra").options(table="topic_date_aggregates", keyspace="swashbucklers").save(mode="overwrite")

  ##write the dataframe as a json file



  topics_date_count.unpersist()
  ##topics_date_count.show()
