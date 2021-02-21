from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, asc 

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

def nTopMovieRating(movies, ratings, n):
    return movies.join(ratings, movies.movieId == ratings.movieId).select("rating", "title").orderBy(desc("rating")).limit(n).collect()

for x in nTopMovieRating(movies, ratings, 10):
    print(x)