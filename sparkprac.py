import pyspark.sql.dataframe
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, split, explode, first, max, desc, asc, countDistinct, struct

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
    averageRatings = ratings.groupBy("movieId").agg(avg("rating"))
    return movies.join(averageRatings, movies.movieId == averageRatings.movieId).select("avg(rating)", "title").orderBy(col("avg(rating)").desc()).limit(n).collect()

#returning top 10 movies watched
def nTopMovieWatches(ratings):
     moviesDF = (ratings.groupBy('movieId').count()).orderBy(desc('count')).limit(10)
     return movies.join(moviesDF, movies.movieId == moviesDF.movieId).select('title').collect()

#returns list of movies by user inputed year... limit it temp
def searchMovieByYear(movies, year, limit):
    return movies.filter(movies.title.contains(year)).limit(limit).collect()

#returns list of movies by user inputed genre... limit it temp
def searchMovieByGenre(movies, genre, limit):
    return movies.filter(movies.title.contains(genre)).limit(limit).collect()

#returns number of movies and genres watched given a user's Id
def searchUserId(movies, ratings, userId):
    noMovieWatched = ratings.groupBy("userId").agg(count("movieId"))
    noGenreWatched = movies.alias('a').join(ratings.alias('b'), movies.movieId == ratings.movieId).withColumn("genre",explode(split("genres","\|"))).drop("genres").groupBy("userId").agg(countDistinct("genre"))
    combined = noMovieWatched.alias('a').join(noGenreWatched.alias('b'), noMovieWatched.userId == noGenreWatched.userId).select("a.userId", "a.count(movieId)","b.count(genre)")
    return combined.filter(combined.userId.contains(userId)).collect()

#returns a pandas series with result of favourite genre for a given user
def findFavGenre(movies, ratings, userId):
    countPerGenreWatched = movies.alias('a').join(ratings.alias('b'), movies.movieId == ratings.movieId).withColumn("genre",explode(split("genres","\|"))).drop("genres").groupBy("userId", "genre").count().groupBy("userId").pivot("genre").agg(max("count"))
    countPandas = countPerGenreWatched.filter(countPerGenreWatched.userId.contains(userId)).toPandas()
    return countPandas.drop("userId", 1).idxmax(axis=1)

#====================TESTS===================
# printing out result of nTopMovie ratings
for x in nTopMovieRating(movies, ratings, 10):
    print(x)

#printing out result of searchMovieByYear
for x in searchMovieByYear(movies, "1992", 10):
    print(x)

#printing out result of searchMovieByGenre
for x in searchMovieByGenre(movies, "Comedy", 10):
    print(x)

#printing top10 movies
print(nTopMovieWatches(ratings))

#printing result of serachUserId
print(searchUserId(movies, ratings, 471))

#printing result of findFavGenre given user Id ... here testing user 118
print(findFavGenre(movies, ratings, "118")[0])