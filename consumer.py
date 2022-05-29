import sys
from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
import ast
import psycopg2
conn = psycopg2.connect("dbname=dbt user=postgres password=sujay123")
cur = conn.cursor()


bootstrap_servers = ['localhost:9092']
topicName = ['COVID', 'BTS', 'IPL', 'ELONMUSK', 'JOHNNYDEPP']
consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
consumer.subscribe(topicName)
try:
    for message in consumer:
        print(message.topic, message.value)
        x = message.value.decode('utf-8')
        x = ast.literal_eval(x)
        print(x["window_id"], x["count"])
        cur.execute("INSERT INTO WIN VALUES(%s , %s,  %s)",
                    (x["window_id"], message.topic, x["count"]))
        conn.commit()
except KeyboardInterrupt:
    cur.execute('select * from win')
    print(cur.fetchall())
    conn.commit()
    conn.close()
    sys.exit()


"""
sql ='''CREATE TABLE WIN(
   WINDOW_NO INT NOT NULL,
   HASH_TAG CHAR(10),
   COUNT_H INT
)'''

cur.execute(sql)
"""
# for i in custom_list:
#     cur.execute("INSERT INTO WIN VALUES(%s , %s %s)", (i[0], i[1], i[2])),


# At the end
