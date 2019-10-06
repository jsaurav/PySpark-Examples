from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StringType,StructType
from pyspark.sql.functions import split,col, explode, lit,sum
from sys import argv

if __name__ == '__main__':
    with SparkSession.builder.appName('SimpleWordCountStreamingApp').getOrCreate() as spark:


        schema_data = "line "
        fields = [StructField(field_name, StringType(), True) for field_name in schema_data.split()]

        # Create the schema for tha DataFrame.
        my_schema = StructType(fields)

        # Source DataFrame after loading the text file.
        source_df = spark.\
                    readStream.\
                    format('socket').\
                    option('host', '172.16.33.109').\
                    option('port', '1234').\
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

        # Final transformed DataFrame after applying groupBy and sum aggregation onto the
        # DataFrame
        output_df = source_df_split_explode.groupBy(col('word')).count()

        # stream
        streaming_df = output_df.\
                       writeStream.\
                       outputMode('complete').\
                       format('console').\
                       start()


        streaming_df.awaitTermination()

