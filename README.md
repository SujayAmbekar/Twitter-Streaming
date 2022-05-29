# Twitter-Streaming
Twitter tweet analysis using spark, kafka and sql

# Problem Statement
1. Collect twitter feed for 30 minutes with any 5 hash tags.
2. Ingest into spark.
3. Use 6 tumbling windows of 5 minutes each.
4. Aggregate count of each of these hash tags for each of the 5 minute windows.
5. Publish them as topics into Kafka.
6. Export topic wise count by window into any database.
