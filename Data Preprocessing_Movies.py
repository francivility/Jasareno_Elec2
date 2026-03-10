# ============================================================
# PySpark Activity: Movies Dataset Analysis
# ============================================================
# HOW TO RUN:
#   1. Put this file and movies.csv in the same folder
#   2. Open PowerShell in that folder
#   3. Type: python pyspark_movies_simple.py
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, round

# ─────────────────────────────────────────────
# STEP 1: Start Spark
# ─────────────────────────────────────────────
# SparkSession is the starting point of every PySpark program
spark = SparkSession.builder \
    .appName("Movies Analysis") \
    .getOrCreate()

# Hide unnecessary logs so output is clean
spark.sparkContext.setLogLevel("ERROR")

print("Spark started!")


# ─────────────────────────────────────────────
# STEP 2: Load the CSV file
# ─────────────────────────────────────────────
# Read the CSV file into a DataFrame (like a table)
# header=True  → first row is column names
# inferSchema  → automatically detects data types
df = spark.read.csv("movies.csv", header=True, inferSchema=True)

print("\n--- LOADED DATA ---")
print("Total movies:", df.count())   # Count total rows
print("Columns:", df.columns)        # Show column names

# Show the first 5 rows
print("\nFirst 5 rows:")
df.show(5, truncate=False)


# ─────────────────────────────────────────────
# STEP 3: Basic Operations
# ─────────────────────────────────────────────

# --- Select specific columns ---
# Only show title, genre, year, and rating
print("\n--- SELECTED COLUMNS ---")
df.select("title", "genre", "year", "rating").show(5)

# --- Filter rows ---
# Show only movies with rating 8.5 or higher
print("\n--- HIGH RATED MOVIES (rating >= 8.5) ---")
df.filter(col("rating") >= 8.5).select("title", "rating").show(10)

# Show only movies from year 2010 onwards
print("\n--- RECENT MOVIES (year >= 2010) ---")
df.filter(col("year") >= 2010).select("title", "year", "rating").show(10)

# --- Data Cleaning ---
# Remove any rows that have missing/null values
df_clean = df.dropna()
print("\n--- DATA CLEANING ---")
print("Rows before cleaning:", df.count())
print("Rows after cleaning: ", df_clean.count())


# ─────────────────────────────────────────────
# STEP 4: SQL Queries
# ─────────────────────────────────────────────
# Register the DataFrame as a table so we can use SQL
df_clean.createOrReplaceTempView("movies")

print("\n--- SQL QUERY 1: Top 5 Most Frequent Genres ---")
spark.sql("""
    SELECT genre,
           COUNT(*) AS total_movies
    FROM movies
    GROUP BY genre
    ORDER BY total_movies DESC
    LIMIT 5
""").show()

print("\n--- SQL QUERY 2: Top 5 Highest Rated Movies ---")
spark.sql("""
    SELECT title,
           genre,
           rating
    FROM movies
    ORDER BY rating DESC
    LIMIT 5
""").show(truncate=False)

print("\n--- SQL QUERY 3: Top 5 Directors with Most Movies ---")
spark.sql("""
    SELECT director,
           COUNT(*) AS total_movies,
           ROUND(AVG(rating), 2) AS avg_rating
    FROM movies
    GROUP BY director
    ORDER BY total_movies DESC
    LIMIT 5
""").show(truncate=False)


# ─────────────────────────────────────────────
# STEP 5: Save Results to CSV
# ─────────────────────────────────────────────
# coalesce(1) = save as a single file instead of many parts

print("\n--- SAVING RESULTS ---")

# Save top genres
top_genres = spark.sql("""
    SELECT genre, COUNT(*) AS total_movies
    FROM movies
    GROUP BY genre
    ORDER BY total_movies DESC
    LIMIT 5
""")
top_genres.coalesce(1).write.mode("overwrite").option("header", True).csv("output/top_genres")
print("Saved: output/top_genres")

# Save top rated movies
top_movies = spark.sql("""
    SELECT title, genre, rating
    FROM movies
    ORDER BY rating DESC
    LIMIT 5
""")
top_movies.coalesce(1).write.mode("overwrite").option("header", True).csv("output/top_movies")
print("Saved: output/top_movies")

# Save cleaned dataset
df_clean.coalesce(1).write.mode("overwrite").option("header", True).csv("output/cleaned_movies")
print("Saved: output/cleaned_movies")

print("\nDone! Check the output/ folder for your CSV files.")

# Stop Spark when done
spark.stop()
