##gets the tweet id for the most retweeted tweets and populates data in cassandra table retweets_aggregates

from pyspark import SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import count
from pyspark.sql.functions import desc


if __name__ == "__main__":
    ##Create a spark session with cassandra
    spark = SparkSession.builder.appName("SparkCassandraApp").config("spark.cassandra.connection.host", "sb-k8s-ingress-1525602578.us-west-2.elb.amazonaws.com").config("spark.cassandra.connection.port", "9042").master("local[2]").getOrCreate();

    ##Load the table as a dataframe
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table="tweets_master", keyspace="swashbucklers").load();
    ##df.show()

    df.cache()
    topic = df.select("topic").distinct()
    array = [i.topic for i in topic.collect()]
    for i in array:
        q="retweets = df.select(\"topic\",\"retweet_count\",\"tweet_id\").filter(\"topic =" +i +"\").sort(desc(\"retweet_count\")).limit(5)"
        exec(q)
        retweets.write.format("org.apache.spark.sql.cassandra").options(table="retweets_aggregates", keyspace="swashbucklers").save(mode="append")

    retweets.unpersist()
