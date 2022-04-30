from curses import window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.streaming import StreamingContext
from pyspark.sql.context import SQLContext
from pyspark import SparkContext
from pyspark.sql import Row
import ast
import json
from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

topic_list = []
hash_list = []
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", client_id='test')
# admin_client.create_topics(new_topics=topic_list, validate_only=False)


def preprocessing(time, rdd):
    global producer
    global topic_list
    global hash_list
    global admin_client
    if rdd.isEmpty():
        return
    # df = rdd.toDF()
    temp = rdd.collect()
    x = ast.literal_eval(temp[0])
    stream_list = []
    for i in x:
        global window_id
        user_name = i[0]
        # content = i[1]
        window_id = i[1]
        hashtag = i[2].upper()
        if hashtag not in hash_list:
            hash_list.append(hashtag)
            try:
                newto = NewTopic(name=hashtag,
                                 num_partitions=1, replication_factor=1)
                topic_list.append(newto)
                admin_client.create_topics(
                    new_topics=[newto], validate_only=False)
            except Exception as e:
                print(e)
        # print(user_name, " has posted ", content, " with hashtag: ", hashtag)
        stream_list.append(hashtag)
    data = {'window_id': window_id}
    print(data, stream_list)
    [producer.send(hashtag, value={'window_id': window_id, 'count': stream_list.count(hashtag)})
     for hashtag in set(stream_list)]


def rdd_test(time, rdd):
    global iter, test_lm, test_sgd, test_mlp, test_mlp, test_clus, test_mnb
    print(f"===================={str(time)}====================")


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    ssc = StreamingContext(sc, 5)
    sqlContext = SQLContext(sc)  # required to create dataframe
    lines = ssc.socketTextStream("localhost", 6100)
    # lines = lines.flatMap(lambda l: l.split(','))
    # lines.pprint()
    # lines.pprint()
    # lines = lines.map(lambda l: l.split(',', 1))
    lines.foreachRDD(preprocessing)
    ssc.start()
    ssc.awaitTermination(500)
    ssc.stop()
