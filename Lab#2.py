from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab2").getOrCreate()

# Dataset: Philippine Public Education Data (DepEd)
# Source: https://www.kaggle.com/datasets/johnjoemmelrodelas/philippine-public-education-data
df = spark.read.csv("philippine_public_education.csv", header=True, inferSchema=True)
df.printSchema()


partition_region = df.repartition("region")
region_summary = partition_region.groupBy("region").count().orderBy("count", ascending=False)
print("Number of Schools per Region:")
region_summary.show()


partition_division = df.repartition(5, "division")
division_summary = partition_division.groupBy("division").count().orderBy("count", ascending=False)
print("Number of Schools per Division:")
division_summary.show()


public_schools = df.filter(df["school_type"] == "Public")
print("Public Schools:")
public_schools.select("school_name", "region", "division").show(10)


sorted_enrollment = df.orderBy("total_enrollment", ascending=False)
print("Schools by Highest Enrollment:")
sorted_enrollment.select("school_name", "region", "total_enrollment").show(10)


ncr_schools = df.filter(df["region"] == "NCR")
print("Schools in NCR:")
ncr_schools.select("school_name", "division", "total_enrollment").show(10)


from pyspark.sql.functions import avg, sum as _sum, count
regional_stats = df.groupBy("region").agg(
    count("school_name").alias("total_schools"),
    _sum("total_enrollment").alias("total_enrolled"),
    avg("total_enrollment").alias("avg_enrollment")
).orderBy("total_enrolled", ascending=False)
print("Regional Education Summary:")
regional_stats.show()

spark.stop()
