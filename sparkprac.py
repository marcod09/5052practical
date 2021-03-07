import pyspark.sql.dataframe
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, split, explode, first, max, desc, asc 

moviesPath = "../practical/ml-latest-small/movies.csv"
ratingsPath = "../practical/ml-latest-small/ratings.csv"

#starting spark session
spark = SparkSession.builder.master("local").appName("movie_Practical") \
    .config("conf-key", "conf-value") \
    .getOrCreate()

#reading in csv files
movies = spark.read.csv(moviesPath,header=True,inferSchema=True)
ratings = spark.read.csv(ratingsPath,header=True,inferSchema=True)

#checking movie.csv schema
movies.printSchema()

#printing out first row
for x in movies.take(1):
    print(x)

#cecking schema for ratings
ratings.printSchema()

#returning top n movies by rating in descending order
def nTopMovieRating(movies, ratings, n):
    averageRatings = ratings.groupBy("movieID").agg(avg("rating"))
    return movies.join(averageRatings, movies.movieId == ratings.movieId).select("avg(rating)", "title").orderBy(desc("avg(rating)")).limit(n).collect()

# printing out result of nTopMovie ratings
for x in nTopMovieRating(movies, ratings, 10):
    print(x)