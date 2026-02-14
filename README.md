# CSL7110_Assign-1


Perfect 👍 I’ve gone through the uploaded files (`WordCount.java`, `WordCountQ9.java`, input/output samples) and combined that with your Spark scripts for Q10–Q12.

Below is a **clean, beginner-friendly README outline** that covers **Q1–Q12**, based on your assignment structure and implemented scripts.

You can copy-paste this into your GitHub `README.md`.

---

# CSL7110 – Big Data Analytics Assignment 1

**Hadoop MapReduce + Apache Spark**

This project contains solutions for **Questions 1–12** of Assignment 1.
It demonstrates:

* Hadoop MapReduce (Java)
* Apache Spark (PySpark)
* HDFS usage
* Text processing
* TF-IDF & Cosine Similarity
* Metadata extraction
* Author influence network (graph-style analysis)

---

# 📂 Project Structure

```
WordCount_Prj/
│
├── WordCount.java              # Q1–Q8 basic Hadoop WordCount
├── WordCountQ9.java            # Q9 WordCount with custom split size & execution time
├── input.txt                   # Sample input file (WordCount)
├── output.txt                  # Sample WordCount output
├── output200.txt               # Additional output sample
└── README.md
```

---

# 🧩 Q1–Q8: Hadoop MapReduce – WordCount

### 📌 File: `WordCount.java`

Implements classic WordCount using:

* `Mapper` → Tokenizes text
* `Reducer` → Aggregates word counts
* Cleans punctuation
* Converts to lowercase

### 🔍 Core Logic

From your code:

```java
String cleanLine = value.toString()
                        .toLowerCase()
                        .replaceAll("[^a-zA-Z ]", " ");
```

This:

* Removes punctuation
* Keeps only alphabetic characters
* Normalizes case

### ▶ Run

```bash
hdfs dfs -put input.txt /user/vboxuser/input
hadoop jar WordCount.jar WordCount /user/vboxuser/input /user/vboxuser/output
```

---

# Q9: Optimized WordCount + Execution Time

###  File: `WordCountQ9.java`

Enhancements over Q1–Q8:

✔ Custom input split size
✔ Execution time measurement

```java
conf.setLong(
  "mapreduce.input.fileinputformat.split.maxsize",
  134217728   // 128MB
);
```

Also measures total runtime:

```java
long startTime = System.currentTimeMillis();
...
long endTime = System.currentTimeMillis();
System.out.println("Total Execution Time: " + (endTime - startTime) + " ms");
```

###  Purpose

To analyze performance impact of different input split sizes.

---

#  Q10 – Metadata Extraction (Spark)

###  File: `metadata_extraction.py`

Extracts from each book:

* Title
* Release Year
* Language
* Encoding

Using **regular expressions**.

### Example Extracted Metadata

| file_name | title | release_year | language | encoding |
| --------- | ----- | ------------ | -------- | -------- |

### 📊 Analysis Performed

* Books released per year
* Most common language
* Average title length

### ▶ Run

```bash
spark-submit metadata_extraction.py
```

---

# Q11 – TF-IDF & Cosine Similarity

###  File: `tfidf_similarity.py`

Steps:

1. Tokenization
2. Stopword removal
3. TF calculation (`HashingTF`)
4. IDF calculation
5. TF-IDF vector generation
6. Cosine similarity between books

### Example Output

```
Top 5 books similar to 10.txt
```

### ▶ Run

```bash
spark-submit tfidf_similarity.py
```

---

#  Q12 – Author Influence Network

###  File: `author_influence.py`

Creates a **graph-like influence network** where:

Two authors are connected if:

```
|year1 - year2| <= X years
```

(Default: X = 5 years)

### Outputs

* Top 5 authors by Out-Degree
* Top 5 authors by In-Degree

This demonstrates:

* Self-join in Spark
* Graph-style edge construction
* Degree computation using groupBy()

### ▶ Run

```bash
spark-submit author_influence.py
```

---

# 🛠 Technologies Used

* Hadoop 3.x
* Apache Spark 3.5.x
* PySpark
* Java (MapReduce)
* Ubuntu VM
* HDFS

---

# Key Learning Outcomes

✔ Hadoop MapReduce architecture
✔ Spark DataFrame API
✔ Text cleaning & regex extraction
✔ TF-IDF implementation
✔ Cosine similarity
✔ Graph-like network modeling in Spark
✔ Performance tuning (split size)

---

# How to Run the Full Project

### 1️⃣ Start Hadoop

```bash
start-dfs.sh
jps
```

### 2️⃣ Run Hadoop WordCount

```bash
hadoop jar WordCount.jar WordCount input output
```

### Run Spark Scripts

```bash
spark-submit metadata_extraction.py
spark-submit tfidf_similarity.py
spark-submit author_influence.py
```

---

# Notes

* Large datasets may produce heavy logs.
* Log level reduced using:

```python
spark.sparkContext.setLogLevel("ERROR")
```

* Influence network is simplified and does not represent true historical influence.

---

# Final Remarks

This assignment demonstrates the evolution from:

Hadoop MapReduce → Spark DataFrames → Analytical & Graph Processing

It showcases scalable text processing, metadata extraction, similarity analysis, and network modeling on large text datasets.

---
