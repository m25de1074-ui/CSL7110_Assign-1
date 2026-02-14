Thie read me file explains:

* Project goal
* File structure
* What each script does
* How to run everything
* Expected outputs

CSL7110 – Big Data Processing with Spark Q10,11,12

## Project Overview

This project demonstrates large-scale text processing and analysis using **Apache Spark** and **HDFS**.

We use a collection of Project Gutenberg books (≈400+ text files) to perform:

1. Metadata Extraction
2. TF-IDF & Cosine Similarity
3. Author Influence Network Analysis

The implementation uses **PySpark (DataFrame API)** and runs in local mode on Hadoop.

---

# Project Structure

```
CSL7110_Spark/
│
├── load_books.py
├── metadata_analysis.py
├── tfidf_similarity.py
├── author_influence_network.py
├── README.md
```

---

# Script Descriptions

## `load_books.py`

### Purpose:

Loads all book `.txt` files from HDFS and verifies dataset integrity.

### Core Functionality:

* Reads all text files from HDFS
* Creates Spark DataFrame
* Displays file count and sample records

### Why It Exists:

Ensures Spark + HDFS setup is working before running analysis.

---

## `metadata_analysis.py`  (Q10)

### Purpose:

Extracts and analyzes metadata from book headers.

### Core Functionality:

* Extracts:

  * Title
  * Release Year
  * Language
  * Encoding
* Cleans missing/invalid values
* Performs basic analysis:

  * Books per year
  * Most common language
  * Average title length

### Output:

Statistical summary of metadata distribution.

---

## `tfidf_similarity.py`  (Q11)

### Purpose:

Finds similar books using TF-IDF and Cosine Similarity.

### Core Functionality:

* Tokenizes text
* Removes stopwords
* Applies:

  * HashingTF
  * IDF
* Computes cosine similarity between books
* Displays top 5 most similar books for a given file

### Output:

Example:

```
Top 5 books similar to 10.txt
```

### Concept Demonstrated:

Text similarity using distributed feature engineering.

---

## `author_influence_network.py`  (Q12)

### Purpose:

Builds a simplified author influence graph.

### Core Functionality:

* Extracts:

  * Author
  * Release Year
* Creates directional edges:

  ```
  Author A → Author B
  ```

  if Author B released a book within 5 years after Author A.
* Calculates:

  * Out-degree (authors influenced)
  * In-degree (authors influenced by others)
* Displays top 5 authors in each category

### Concept Demonstrated:

Graph-like analysis using Spark DataFrames.

---

# System Requirements

* Ubuntu / Linux
* Java 11
* Hadoop (HDFS running)
* Apache Spark 3.x
* Python 3.x
* numpy installed (`sudo apt install python3-numpy`)

---

# How to Run

###  Start Hadoop

```bash
start-dfs.sh
```

Check:

```bash
jps
```

You should see:

```
NameNode
DataNode
SecondaryNameNode
```

---

###  Run Scripts

From project directory:

```bash
spark-submit metadata_analysis.py
spark-submit tfidf_similarity.py
spark-submit author_influence_network.py
```

---

# Concepts Covered

* Distributed text processing
* Regular expression extraction
* DataFrame transformations
* TF-IDF feature engineering
* Cosine similarity
* Graph construction via self-join
* Degree centrality analysis

---

# Limitations

* Influence is based only on publication year (simplified model)
* Full pairwise joins may not scale to millions of books
* Metadata extraction depends on consistent formatting

---

# Learning Outcomes

This project demonstrates:

* How to use Spark with HDFS
* Large-scale text analytics
* Feature engineering for NLP
* Building graph-like relationships without dedicated graph libraries
* Understanding scalability considerations in distributed systems
