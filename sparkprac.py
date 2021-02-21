from pyspark.sql import SparkSession

moviesPath = "../practical/ml-latest-small/movies.csv"
ratingsPath = "../practical/ml-latest-small/ratings.csv"

spark = SparkSession.builder.master("local").appName("movie_Practical") \
    .config("conf-key", "conf-value") \
    .getOrCreate()

movies = spark.read.format("csv").load(moviesPath)
ratings = spark.read.format("csv").load(ratingsPath)