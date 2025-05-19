import time
import re
from collections import defaultdict
from pyspark.sql import SparkSession


# Initialize Spark session
spark = SparkSession.builder.master("local").appName("WikimediaPageViews").getOrCreate()
data_rdd = spark.sparkContext.textFile("pagecounts-20160101-000000_parsed.out")

def parse_line(line):
    fields = line.split()
    if len(fields) == 4:
        project_code = fields[0]
        page_title = fields[1]
        page_hits = int(fields[2])
        page_size = int(fields[3])
        return (project_code, page_title, page_hits, page_size)
    else:
        return None


parsed_rdd = data_rdd.map(parse_line).filter(lambda x: x is not None)

parsed_rdd.persist()


###################################################################

# Q1 Compute the min, max, and average page size.

# MapReduce
def compute_page_size_stats(rdd):
    page_sizes = rdd.map(lambda x: x[3])
    min_size = page_sizes.min()
    max_size = page_sizes.max()
    avg_size = page_sizes.mean()

    return min_size, max_size, avg_size


# Loops
def compute_page_size_stats_loop(rdd):
    page_sizes = rdd.map(lambda x: x[3])
    min_size = float('inf')
    max_size = float('-inf')
    total_size = 0
    count = 0

    for size in page_sizes.collect():
        if size < min_size:
            min_size = size
        if size > max_size:
            max_size = size
        total_size += size
        count += 1

    avg_size = total_size / count if count > 0 else 0

    return min_size, max_size, avg_size

###################################################################

#Q2 Compute the number of page titles that start with the article “The”. How many of those
# page titles are not part of the English project (Pages that are part of the English project
# have “en” as first field)?

# MapReduce

def count_titles_starting_with_the(rdd):
    # Filter titles starting with "The"
    titles_starting_with_the = rdd.filter(lambda x: x[1].startswith("The"))

    # Count total titles starting with "The"
    total_count = titles_starting_with_the.count()

    # Count titles not part of the English project
    non_english_count = titles_starting_with_the.filter(lambda x: x[0] != "en").count()

    return total_count, non_english_count

# Loops
def count_titles_starting_with_the_loop(rdd):
    total_count = 0
    non_english_count = 0

    for record in rdd.collect():
        if record[1].startswith("The"):
            total_count += 1
            if record[0] != "en":
                non_english_count += 1

    return total_count, non_english_count

###################################################################

# Q3 Determine the number of unique terms appearing in the page titles. Note that in page
# titles, terms are delimited by “_” instead of a white space. You can use any number of
# normalization steps (e.g. lowercasing, removal of non-alphanumeric characters).

# MapReduce
def count_unique_terms(rdd):
    # Extract terms from page titles
    terms = rdd.flatMap(lambda x: re.findall(r'\w+', x[1].lower()))

    # Count unique terms
    term_counts = terms.map(lambda term: (term, 1)).reduceByKey(lambda a, b: a + b)

    # Filter terms that appear only once and count them
    unique_terms_count = term_counts.filter(lambda x: x[1] == 1).count()

    return unique_terms_count


# Loops
def count_unique_terms_loop(rdd):
    unique_terms = defaultdict(int)

    for record in rdd.collect():
        terms = re.findall(r'\w+', record[1].lower())
        for term in terms:
            unique_terms[term] += 1

    unique_terms_count = sum(1 for count in unique_terms.values() if count == 1)

    return unique_terms_count

###################################################################

# Q4 Extract each title and the number of times it was repeated.

# MapReduce
def count_title_repetitions(rdd):
    # Map titles to counts
    title_counts = rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)

    # Collect results
    return title_counts.collect()

# Loops
def count_title_repetitions_loop(rdd):
    title_counts = defaultdict(int)

    for record in rdd.collect():
        title_counts[record[1]] += 1

    return title_counts.items()

###################################################################


# Q5 Combine between data of pages with the same title and save each pair of pages data
# in order to display them.

# MapReduce
def combine_page_data(rdd):
    # Combine data for pages with the same title
    combined_data = rdd.map(lambda x: (x[1], (x[0], x[2], x[3]))).reduceByKey(lambda a, b: (a[0], a[1] + b[1], a[2] + b[2]))

    # Collect results
    return combined_data.collect()

# Loops

def combine_page_data_loop(rdd):
    combined_data = defaultdict(lambda: [None, 0, 0])

    for record in rdd.collect():
        title = record[1]
        if combined_data[title][0] is None:
            combined_data[title][0] = record[0]
        combined_data[title][1] += record[2]
        combined_data[title][2] += record[3]

    return [(title, data) for title, data in combined_data.items()]

###################################################################

# compare their performance in terms of time, create a document.csv includes all the results of each query:

def measure_performance(func, rdd):
    start_time = time.time()
    result = func(rdd)
    end_time = time.time()
    return result, end_time - start_time

def save_results_to_csv(results, filename):
    with open(filename, 'w') as f:
        for result in results:
            f.write(','.join(map(str, result)) + '\n')


###################################################################

def main():
    # Measure performance for each query
    results = []

   # Q1
    print("Running Q1 MapReduce...")
    result_map, time_mp_1 = measure_performance(compute_page_size_stats, parsed_rdd)
    min_size_map, max_size_map, avg_size_map = result_map
    print(f"Q1 MapReduce completed: min: {min_size_map}, max: {max_size_map}, avg: {avg_size_map}, time: {time_mp_1}")

    print("Running Q1 Loops...")
    result_loop, time_loop_1 = measure_performance(compute_page_size_stats_loop, parsed_rdd)
    min_size_loop, max_size_loop, avg_size_loop = result_loop
    print(f"Q1 Loops completed: min: {min_size_loop}, max: {max_size_loop}, avg: {avg_size_loop}, time: {time_loop_1}")

    results.append(("Q1", "MapReduce", min_size_map, max_size_map, avg_size_map, time_mp_1))
    results.append(("Q1", "Loops", min_size_loop, max_size_loop, avg_size_loop, time_loop_1))

    # Q2
    print("Running Q2 MapReduce...")
    result_map, time_mp_2 = measure_performance(count_titles_starting_with_the, parsed_rdd)
    total_count_map, non_english_count_map = result_map
    print(f"Q2 MapReduce completed: {total_count_map}, {non_english_count_map}, time: {time_mp_2}")

    print("Running Q2 Loops...")
    result_loop, time_loop_2 = measure_performance(count_titles_starting_with_the_loop, parsed_rdd)
    total_count_loop, non_english_count_loop = result_loop
    print(f"Q2 Loops completed: {total_count_loop}, {non_english_count_loop}, time: {time_loop_2}")

    results.append(("Q2", "MapReduce", total_count_map, non_english_count_map, time_mp_2))
    results.append(("Q2", "Loops", total_count_loop, non_english_count_loop, time_loop_2))

    # Q3
    print("Running Q3 MapReduce...")
    unique_terms_count_map, time_mp_3 = measure_performance(count_unique_terms, parsed_rdd)
    print(f"Q3 MapReduce completed: {unique_terms_count_map}, time: {time_mp_3}")

    print("Running Q3 Loops...")
    unique_terms_count_loop, time_loop_3 = measure_performance(count_unique_terms_loop, parsed_rdd)
    print(f"Q3 Loops completed: {unique_terms_count_loop}, time: {time_loop_3}")

    results.append(("Q3", "MapReduce", unique_terms_count_map, time_mp_3))
    results.append(("Q3", "Loops", unique_terms_count_loop, time_loop_3))

    # Q4
    print("Running Q4 MapReduce...")
    title_repetitions_map, time_mp_4 = measure_performance(count_title_repetitions, parsed_rdd)
    print(f"Q4 MapReduce completed: {len(title_repetitions_map)} items, time: {time_mp_4}")

    print("Running Q4 Loops...")
    title_repetitions_loop, time_loop_4 = measure_performance(count_title_repetitions_loop, parsed_rdd)
    print(f"Q4 Loops completed: {len(title_repetitions_loop)} items, time: {time_loop_4}")

    results.append(("Q4", "MapReduce", len(title_repetitions_map), time_mp_4))
    results.append(("Q4", "Loops", len(title_repetitions_loop), time_loop_4))

    # Q5
    print("Running Q5 MapReduce...")
    combined_data_map, time_mp_5 = measure_performance(combine_page_data, parsed_rdd)
    print(f"Q5 MapReduce completed: {len(combined_data_map)} items, time: {time_mp_5}")

    print("Running Q5 Loops...")
    combined_data_loop, time_loop_5 = measure_performance(combine_page_data_loop, parsed_rdd)
    print(f"Q5 Loops completed: {len(combined_data_loop)} items, time: {time_loop_5}")

    results.append(("Q5", "MapReduce", len(combined_data_map), time_mp_5))
    results.append(("Q5", "Loops", len(combined_data_loop), time_loop_5))

    # Save results to CSV
    print("Saving results to CSV...")
    save_results_to_csv(results, 'results/results.csv') 
    print("Results saved successfully!")

    spark.stop()

###################################################################

main()

