from pyspark.sql import SparkSession
from pyspark.sql.functions import col,split, explode, lower,desc

with SparkSession.builder.appName('Spark Kafka Integration').master('local[*]').getOrCreate() as spark:

    tweets_df_raw = spark.\
                        readStream.\
                        format('kafka').\
                        option('kafka.bootstrap.servers', 'localhost:9092').\
                        option('subscribe', 'india-tweets').\
                        option('startingOffsets', 'earliest').\
                        load()

    tweets_df = tweets_df_raw.selectExpr("cast(value AS STRING)")


    # In this transformation we are applying filter over the source dataframe and
    # filtering out all the tweets which do not contains #.
    tweets_stream_filtered = tweets_df.filter(col('value').contains('#'))

    # In this transformation we are splitting our tweet into words and transformed
    # data frame will have an array of words in each row.
    tweet_stream_words = tweets_stream_filtered.select(split(col('value'), ' ').alias('tweet_words'))

    # In this transformation we are exploding each row, and all the words present in the array
    # will be each inserted as a row.
    tweet_stream_exploded_words = tweet_stream_words.select(explode(col('tweet_words')).alias('tweet_word'))

    # In this transformation we are filtering all the rows which don't start with '#'. So, we
    # just want to keep all the hashtags.
    tweet_stream_hashtags = tweet_stream_exploded_words.filter(col('tweet_word').contains('#'))

    # In this transformation we are doing a group by on hashtag and taking the count.
    trending_hashatags_grouped = tweet_stream_hashtags.groupBy(lower(col('tweet_word')).alias('tweet_word')).count().sort(desc('count'))

    # Final streaming data frame is created after setting up the sink properties and calling
    # start() on it to start the streaming.
    # possible modes are complete , append and update.
    trending_hashatags_console = trending_hashatags_grouped. \
        writeStream. \
        outputMode('complete'). \
        format('console'). \
        trigger(processingTime = '180 seconds').\
        option('truncate','false').\
        start()

    # Write to kafka topic
    '''trending_hashatags_kafka = trending_hashatags_grouped.\
        selectExpr('cast((tweet_word, count)as STRING) as value').\
        writeStream. \
        format('kafka'). \
        outputMode('complete').\
        option('kafka.bootstrap.servers','localhost:9092'). \
        option('topic', 'hashtags').\
        option('checkpointLocation', '/home/saurabhjain/data/checkpointLocation').\
        start()'''

    # It will prevent the process from exiting while query is active.
    trending_hashatags_console.awaitTermination()
    #trending_hashatags.awaitTermination()

