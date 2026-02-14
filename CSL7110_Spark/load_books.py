from pyspark.sql import SparkSession
from pyspark.sql import Row
import os

def main():
    spark = SparkSession.builder \
        .appName("CSL7110_Load_Books") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("\n========== LOADING DATASET FROM HDFS ==========\n")
    hdfs_path = "hdfs://localhost:9000/user/vboxuser/D184MB/*.txt"
    rdd = spark.sparkContext.wholeTextFiles(hdfs_path)
    books_rdd = rdd.map(lambda x: Row(
        file_name=os.path.basename(x[0]),
        text=x[1]
    ))
    books_df = spark.createDataFrame(books_rdd)
    books_df.cache()
    total_books = books_df.count()
    print("Total books loaded:", total_books)
    print("\nSchema:")
    books_df.printSchema()
    print("\nSample file names:")
    books_df.select("file_name").show(10, truncate=False)
    print("\n========== DATA LOADING COMPLETE ==========\n")
    spark.stop()
if __name__ == "__main__":
    main()
