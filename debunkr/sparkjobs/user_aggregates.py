##the script generates user_aggregates for each topic
##updates cassandra tables swashbucklers.user_aggregates

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
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

    ##aggregate over various states for a given topic
    df.cache()
    topic = df.select("topic").distinct()
    array = [i.topic for i in topic.collect()]
    for i in array:
        q="user_aggregates = df.select(\"topic\",\"user_id\").filter(\"topic =" +i +"\").groupBy(\"topic\",\"user_id\").agg(count(\"user_id\").alias(\"count\")).na.drop().sort(desc(\"count\")).limit(10)"
        exec(q)
        user_aggregates.write.format("org.apache.spark.sql.cassandra").options(table="user_aggregates", keyspace="swashbucklers").save(mode="append")

    df.unpersist()
    ##topics_count.show()
