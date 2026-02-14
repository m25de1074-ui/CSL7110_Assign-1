from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lower, regexp_replace, udf
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import DoubleType
import os
def main():
    spark = SparkSession.builder \
        .appName("CSL7110_TFIDF_Similarity") \
        .master("local[*]") \
        .config("spark.python.worker.reuse", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("\n========== TF-IDF AND COSINE SIMILARITY ==========\n")
    hdfs_path = "hdfs://localhost:9000/user/vboxuser/D184MB/*.txt"
    rdd = spark.sparkContext.wholeTextFiles(hdfs_path)
    books_rdd = rdd.map(lambda x: Row(
        file_name=os.path.basename(x[0]),
        text=x[1]
    ))
    books_df = spark.createDataFrame(books_rdd)
    books_df = books_df.withColumn(
        "clean_text",
        regexp_replace(col("text"),
                       r"(?s)\*\*\* START OF.*?\*\*\* END OF.*?\*\*\*",
                       "")
    )

    books_df = books_df.withColumn("clean_text", lower(col("clean_text")))

    books_df = books_df.withColumn(
        "clean_text",
        regexp_replace(col("clean_text"), r"[^a-z\s]", "")
    )
    tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
    words_df = tokenizer.transform(books_df)

    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    filtered_df = remover.transform(words_df)
    hashingTF = HashingTF(
        inputCol="filtered_words",
        outputCol="raw_features",
        numFeatures=10000
    )

    featurized_df = hashingTF.transform(filtered_df)
    idf = IDF(inputCol="raw_features", outputCol="tfidf_features")
    idf_model = idf.fit(featurized_df)
    tfidf_df = idf_model.transform(featurized_df)

    print("TF-IDF computation completed.\n")
    def cosine_similarity(v1, v2):
        dot = float(v1.dot(v2))
        norm1 = float(v1.norm(2))
        norm2 = float(v2.norm(2))
        if norm1 == 0.0 or norm2 == 0.0:
            return 0.0
        return dot / (norm1 * norm2)
    cosine_udf = udf(cosine_similarity, DoubleType())
    book_vectors = tfidf_df.select("file_name", "tfidf_features")
    book_pairs = book_vectors.alias("a").join(
        book_vectors.alias("b"),
        col("a.file_name") < col("b.file_name")
    ).withColumn(
        "similarity",
        cosine_udf(col("a.tfidf_features"),
                   col("b.tfidf_features"))
    )
    print("Cosine similarity calculated.\n")
    target_book = "10.txt"
    similar_books = book_pairs.filter(
        (col("a.file_name") == target_book) |
        (col("b.file_name") == target_book)
    ).orderBy(col("similarity").desc())
    print(f"Top 5 books similar to {target_book}:\n")
    similar_books.select(
        "a.file_name",
        "b.file_name",
        "similarity"
    ).show(5, truncate=False)
    print("\n========== PROCESS COMPLETE ==========\n")
    spark.stop()
if __name__ == "__main__":
    main()

