from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, trim, abs as sql_abs, count, input_file_name
from pyspark.sql.types import IntegerType
spark = SparkSession.builder \
    .appName("CSL7110_Author_Influence_Network") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("\n========== AUTHOR INFLUENCE NETWORK ==========\n")
books_df = spark.read.text(
    "hdfs://localhost:9000/user/vboxuser/D184MB/*.txt",
    wholetext=True
)
books_df = books_df.withColumn("file_name", input_file_name())
metadata_df = books_df.select(
    regexp_extract(col("value"), r"(?i)Author:\s*(.*)", 1).alias("author"),
    regexp_extract(col("value"), r"(?i)Release Date:.*?(\d{4})", 1).alias("release_year")
)
metadata_df = metadata_df \
    .withColumn("author", trim(col("author"))) \
    .withColumn("release_year", col("release_year").cast(IntegerType())) \
    .filter((col("author") != "") & col("release_year").isNotNull()) \
    .dropDuplicates()
print("Sample Extracted Metadata:")
metadata_df.show(10, truncate=False)
X = 5
a1 = metadata_df.alias("a1")
a2 = metadata_df.alias("a2")
influence_df = a1.join(
    a2,
    (col("a1.author") != col("a2.author")) &
    (col("a2.release_year") > col("a1.release_year")) &
    (col("a2.release_year") - col("a1.release_year") <= X)
    #(sql_abs(col("a1.release_year") - col("a2.release_year")) <= X)
).select(
    col("a1.author").alias("author1"),
    col("a2.author").alias("author2")
).dropDuplicates()
out_degree = influence_df.groupBy("author1") \
    .agg(count("author2").alias("out_degree")) \
    .orderBy(col("out_degree").desc())
in_degree = influence_df.groupBy("author2") \
    .agg(count("author1").alias("in_degree")) \
    .orderBy(col("in_degree").desc())
print("\nTop 5 Authors by Out-Degree:")
out_degree.show(5, truncate=False)

print("\nTop 5 Authors by In-Degree:")
in_degree.show(5, truncate=False)

print("\n========== PROCESS COMPLETE ==========\n")

spark.stop()

