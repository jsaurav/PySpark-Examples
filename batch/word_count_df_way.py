from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StringType,StructType
from pyspark.sql.functions import split,col, explode, lit,sum
from sys import argv

if __name__ == '__main__':
    with SparkSession.builder.appName('DFWordCountApp').getOrCreate() as spark:
        schema_data = "line "
        fields = [StructField(field_name, StringType(), True) for field_name in schema_data.split()]

        # Create the schema for tha DataFrame.
        my_schema = StructType(fields)

        # Source DataFrame after loading the text file.
        if __name__ == '__main__':
            source_df = spark.\
                        read.\
                        schema(my_schema).\
                        csv(argv[1])

        # Transformed DataFrame after applying the split onto the row to convert
        # each row into an array of words after split.
        source_df_split = source_df.select(split(col("line"), " ").alias('words'))

        # Transformed DataFrame after applying the explode column to transform each word
        # into a separate row.
        source_df_split_explode = source_df_split.select(explode(col("words")).alias('word'))

        # Transformed DataFrame after adding new count Column with initial value 1
        # to the DataFrame.
        source_df_split_explode_with_count_column = source_df_split_explode.withColumn('count', lit(1))

        # Final transformed DataFrame after applying groupBy and sum aggregation onto the
        # DataFrame
        output_df = source_df_split_explode_with_count_column.groupBy(col('word')).agg(sum('count').alias('Total Count'))

        # Calling show action to print the DataFrame.
        output_df.show(10, False)