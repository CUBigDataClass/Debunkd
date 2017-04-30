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
  spark = SparkSession.builder.appName("SparkCassandraApp").config("spark.cassandra.connection.host", "sb-k8s-ingress-1525602578.us-west-2.elb.amazonaws.com").config("spark.cassandra.connection.port", "9042").master("spark://sb-k8s-ingress-1525602578.us-west-2.elb.amazonaws.com:8080").getOrCreate();

  ##read table as a dataframe
  df = spark.read.format("org.apache.spark.sql.cassandra").options(table="tweets_master", keyspace="swashbucklers").load();

  ##aggregate over dates for a given topic
  topics_date_count = df.select("topic","posted_time").groupBy("topic", "posted_time").agg(count("posted_time").alias("count")).na.drop()

  ##update the cassandra table with the counts
  topics_date_count.write.format("org.apache.spark.sql.cassandra").options(table="date_aggregates", keyspace="swashbucklers").save(mode="overwrite")

  topics_date_count.unpersist()
  ##topics_date_count.show()
