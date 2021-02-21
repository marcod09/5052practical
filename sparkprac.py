from pyspark.sql import SparkSession

moviesPath = "../practical/ml-latest-small/movies.csv"
ratingsPath = "../practical/ml-latest-small/ratings.csv"

spark = SparkSession.builder.master("local").appName("movie_Practical") \
    .config("conf-key", "conf-value") \
    .getOrCreate()

movies = spark.read.csv(moviesPath,header=True,inferSchema=True)
ratings = spark.read.csv(ratingsPath,header=True,inferSchema=True)

movies.printSchema()

for x in movies.take(1):
    print(x)

ratings.printSchema()