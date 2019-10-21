from pyspark.sql import SparkSession
from pyspark.sql.functions import col,split,size,lit,sum,desc,trim
from pyspark.sql.types import StructField,StringType,StructType

COLUMN_NAME = 'value'
FRIENDS_FILE_PATH = 'c:/saurav/self/learning/spark/data/superhero/marvel-graph.txt'
NAMES_FILE_PATH = 'c:/saurav/self/learning/spark/data/superhero/marvel-names.txt'

with SparkSession.builder.appName('SuperHero').master('local[*]').getOrCreate() as spark:

    # Overriding the default number of shuffle partitions.
    spark.conf.set('spark.sql.shuffle.partitions', 12)

    # Defining schema fields for the schema.
    schema_fields = [StructField(COLUMN_NAME,StringType(),True)]

    # Created schema type from the schema fields.
    schema_type = StructType(schema_fields)

    # Created super_heroes_source_data data frame after reading the source file.
    # e.g. sample data looks like this.
    super_heroes_source_data = spark.read.format('text').\
                                            schema(schema_type).\
                                            load(FRIENDS_FILE_PATH)

    # Splitting the actual column into 2 columns , 1st column contains the hero_id and
    # second column contains the total number of friends that superhero has.
    super_heroes_friends_count = super_heroes_source_data.select(
                                                                (split(col(COLUMN_NAME), ' ')[0]).\
                                                                                        alias('hero_id'),
                                                                lit(size(split(col(COLUMN_NAME), ' ')) - 1).\
                                                                                        alias('num_of_friends')
                                                                )

    # Grouped by on the hero_id to get the total number of friends of that hero_id.
    super_heroes_grouped = super_heroes_friends_count.groupBy(col('hero_id')).\
                                                        agg(sum(col('num_of_friends')).\
                                                        alias('total_friends')).\
                                                        sort((desc('total_friends')))


    # Created another data frame which has the mapping of hero_id with the hero_name.
    super_hero_names_source_data = spark.read.format('text').\
                                    schema(schema_type).\
                                    load(NAMES_FILE_PATH)

    # Splitting the actual column value, into 2 new column, where 1st column will contain hero_id
    # and 2nd column will contain hero_name.
    super_hero_names_transformed = super_hero_names_source_data.select(
                                                             trim(split(col(COLUMN_NAME), '"')[0]).alias('hero_id'),
                                                             trim(split(col(COLUMN_NAME), '"')[1]).alias('hero_name')
                                                            )

    # Was trying to get the values from the row in other way but it didn't work.
    #super_hero_names_1 = super_hero_names_transformed.select(expr("substring(value,1,length(value)-1)"))


    # Joined 2 dataframes on hero_id column to get the hero_id , hero_name and number_of_friends in the final
    # data frame.
    super_heroes_joined = super_heroes_grouped.join(super_hero_names_transformed,
                                                super_heroes_grouped.hero_id ==  super_hero_names_transformed.hero_id).\
                                                drop(super_hero_names_transformed.hero_id)

    # printing the result on the console.
    super_heroes_joined.show(10, False)

