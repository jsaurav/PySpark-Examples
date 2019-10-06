from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,sum

with SparkSession \
    .builder \
    .master('local[2]') \
    .config('spark.sql.shuffle.partitions', 25) \
    .getOrCreate() as spark:

    ratingsSourceDF = spark.read.format('csv') \
                                .option('inferSchema', 'true') \
                                .option('header', 'true') \
                                .load('C:\\saurav\\self\\learning\\spark\\udemy\\movie-data\\ml-20m\\ratings.csv')

    ratingsExtraColumnDF = ratingsSourceDF.withColumn('count', lit(1))

    ratingsGroupedDF = ratingsExtraColumnDF.groupBy(col('rating')).agg(sum(col('count'))).sort(col('rating'))

    ratingsGroupedDF.show()
