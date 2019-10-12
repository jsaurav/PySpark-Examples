from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col, explode, lit,sum
from sys import argv

if __name__ == '__main__':
    with SparkSession.builder.appName('SimpleWordCountStreamingApp').getOrCreate() as spark:

        # Source DataFrame which will read the data from the web socket running
        # on the server. Both the server and host name will be provided on the command line.
        # Important thing to note here is that socket source doesn't support user defined
        # schema.

        # e.g. you can use netcat to act as a data server. nc -lk 1234
        source_df = spark.\
                    readStream.\
                    format('socket').\
                    option('host', argv[1]).\
                    option('port', argv[2]).\
                    load()

        # Transformed DataFrame after applying the split onto the row to convert
        # each row into an array of words after split.
        source_df_split = source_df.select(split(source_df.value, " ").alias('words'))

        # Transformed DataFrame after applying the explode column to transform each word
        # into a separate row.
        source_df_split_explode = source_df_split.select(explode(col("words")).alias('word'))

        # Transformed DataFrame after adding new count Column with initial value 1
        # to the DataFrame.
        # source_df_split_explode_with_count_column = source_df_split_explode.withColumn('count', lit(1))

        #  Transformed DataFrame after applying groupBy and sum aggregation onto the
        # DataFrame
        output_df = source_df_split_explode.groupBy(col('word')).count()

        # Final streaming data frame is created after setting up the sink properties.
        # possible modes are complete , append and update.
        streaming_df = output_df.\
                       writeStream.\
                       outputMode('complete').\
                       format('console').\
                       start()

        # It will prevent the process from exiting while query is active.
        streaming_df.awaitTermination()

