from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, length, avg, when
from pyspark.sql import Row
import os

def main():

    spark = SparkSession.builder \
        .appName("CSL7110_Metadata_Extraction") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("\n========== METADATA EXTRACTION ==========\n")

    # ⚠ Replace port if yours is different (9000 or 8020)
    hdfs_path = "hdfs://localhost:9000/user/vboxuser/D184MB/*.txt"

    # Load full books from HDFS
    rdd = spark.sparkContext.wholeTextFiles(hdfs_path)

    books_rdd = rdd.map(lambda x: Row(
        file_name=os.path.basename(x[0]),
        text=x[1]
    ))

    books_df = spark.createDataFrame(books_rdd)
    books_df = books_df.withColumn(
        "title",
        regexp_extract(col("text"), r"(?im)^Title:\s*(.+)$", 1)
    )
    books_df = books_df.withColumn(
        "release_year",
        regexp_extract(col("text"), r"(?im)^Release Date:.*?(\d{4})", 1)
    )
    books_df = books_df.withColumn(
        "language",
        regexp_extract(col("text"), r"(?im)^Language:\s*(.+)$", 1)
    )
    books_df = books_df.withColumn(
        "encoding",
        regexp_extract(col("text"), r"(?im)^Character set encoding:\s*(.+)$", 1)
    )
    books_df = books_df.withColumn(
        "release_year",
        when(col("release_year") == "", None).otherwise(col("release_year"))
    )
    books_df = books_df.withColumn(
        "language",
        when(col("language") == "", None).otherwise(col("language"))
    )
    books_df = books_df.withColumn(
        "title",
        when(col("title") == "", None).otherwise(col("title"))
    )

    books_df = books_df.withColumn(
        "encoding",
        when(col("encoding") == "", None).otherwise(col("encoding"))
    )
    print("Sample Extracted Metadata:\n")
    books_df.select("file_name", "title", "release_year", "language", "encoding") \
            .show(10, truncate=False)
    print("\nBooks Released Per Year:\n")
    books_df.groupBy("release_year") \
            .count() \
            .orderBy("release_year") \
            .show()
    print("\nMost Common Language:\n")
    books_df.groupBy("language") \
            .count() \
            .orderBy(col("count").desc()) \
            .show(5)
    print("\nAverage Title Length (in characters):\n")
    books_df.select(avg(length(col("title")))).show()
    print("\n========== METADATA ANALYSIS COMPLETE ==========\n")
    spark.stop()
if __name__ == "__main__":
    main()

