# Wikimedia Pageviews Spark Analysis

This repository contains a Spark-based analysis of Wikimedia project page view statistics.  
We explore and process real page view data using two approaches:
- The **MapReduce paradigm** in Spark
- **Spark procedural loops (collect + local loops)**

We compare both methods in terms of performance (execution time) and correctness across a set of analytical tasks.

## 📊 Dataset

The dataset is a snapshot of Wikimedia page view statistics generated between **00:00–01:00 AM on January 1, 2016**.  
Each line of the dataset contains the following fields (space-delimited):

| Field        | Description                                   |
|--------------|-----------------------------------------------|
| Project code | The project identifier (e.g., `en`, `de`, `fr`) |
| Page title   | The title of the page (underscores instead of spaces) |
| Page hits    | Number of requests during the hour |
| Page size    | Size of the page in bytes |

**Example record**:
en Fresh-water_ecosystem 1 19279


## 🛠️ Features & Functions

The application computes the following:

1. **Page Size Statistics**
   - Compute minimum, maximum, and average page size

2. **Titles Starting with 'The'**
   - Count page titles that start with the article `The`
   - Count how many of those are not part of the English project (`en`)

3. **Unique Terms in Titles**
   - Count unique terms in all page titles  
   _(Terms are delimited by `_`; normalization applied: lowercasing, non-alphanumerics removed)_

4. **Title Frequencies**
   - Extract each page title and the number of times it was repeated

5. **Combine Same Titles**
   - Merge data for pages with the same title  
   _(Group by title and pair their project codes, hits, sizes)_

## 🚀 Technologies

- Apache Spark (tested with Spark 3.x)
- Python (PySpark)
- Google Colab
- Dataset from Wikimedia pageviews (2016)


## ⚡ Performance Evaluation

Both implementations (MapReduce vs Spark Loops) are benchmarked and compared.  
Results, execution time, and insights are available in the `results.csv`.


