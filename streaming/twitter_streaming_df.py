from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql.functions import col,split,explode,desc,lower
from sys import argv

schema_data = 'tweet'

with SparkSession.builder.appName('Twitter Stream Processing').master('local[*]').getOrCreate() as spark:

    # Source DataFrame which will read the tweets from the web socket running
    # on the tcp server. Both the server and host name will be provided on the command line.
    # We are streaming twitter data to this web socket and this socket is acting as
    # the source for our spark streaming job.
    # Important thing to note here is that socket source doesn't support user defined
    # schema.

    tweets_stream_df = spark.\
                            readStream.\
                            format('socket').\
                            option('host', argv[1]).\
                            option('port', argv[2]).\
                            load()

    # In this transformation we are applying filter over the source dataframe and
    # filtering out all the tweets which do not starts with #.
    tweets_stream_filtered = tweets_stream_df.filter(col('value').startswith('#'))

    # In this transformation we are splitting our tweet into words and transformed
    # data frame will have an array of words in each row.
    tweet_stream_words = tweets_stream_filtered.select(split(col('value'), ' ').alias('tweet_words'))

    # In this transformation we are exploding each row, and all the words present in the array
    # will be each inserted as a row.
    tweet_stream_exploded_words = tweet_stream_words.select(lower(explode(col('tweet_words'))).alias('tweet_word'))

    # In this transformation we are filtering all the rows which don't start with '#'. So, we
    # just want to keep all the hashtags.
    tweet_stream_hashtags = tweet_stream_exploded_words.filter(col('tweet_word').contains('#'))

    # In this transformation we are doing a group by on hashtag and taking the count.
    trending_hashatags_grouped = tweet_stream_hashtags.groupBy(col('tweet_word')).count().sort(desc('count'))

    # Final streaming data frame is created after setting up the sink properties and calling
    # start() on it to start the streaming.
    # possible modes are complete , append and update.
    trending_hashatags = trending_hashatags_grouped.\
                                                    writeStream.\
                                                    outputMode('complete').\
                                                    format('console').\
                                                    start()

    # It will prevent the process from exiting while query is active.
    trending_hashatags.awaitTermination()