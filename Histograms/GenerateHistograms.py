import pandas as pd
import matplotlib.pyplot as plt
import re
import numpy as np
import seaborn as sns
import os

# Set the style of seaborn for the plots
sns.set(style="whitegrid")

# Define the file paths for Cassandra and Neo4j data
cassandra_csv_path = [
    "Cassandra/ResponseTimes/cassandra_times_of_response_first_execution.csv",
    "Cassandra/ResponseTimes/cassandra_response_times_average_30.csv",
]

neo4j_csv_paths = [
    "Neo4j/ResponseTimes/neo4j_times_of_response_first_execution.csv",
    "Neo4j/ResponseTimes/neo4j_response_times_average_30.csv",
]

# Check if files exist
for path in cassandra_csv_path + neo4j_csv_paths:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"The file {path} does not exist.")

# Load data from CSV files
data_cassandra_first_execution = pd.read_csv(cassandra_csv_path[0], sep=',')
data_cassandra_avg_30 = pd.read_csv(cassandra_csv_path[1], sep=',')
data_neo4j_first_execution = pd.read_csv(neo4j_csv_paths[0], sep=',')
data_neo4j_avg_30 = pd.read_csv(neo4j_csv_paths[1], sep=',')

# Define dataset sizes and queries to analyze
dataset_sizes = ['100%', '75%', '50%', '25%']
queries = ['Query 1', 'Query 2', 'Query 3', 'Query 4']

# Define colors for the plots
color_cassandra = '#2b93db'  # Vivid blue
color_neo4j = '#4bce4b'     # Bright lime

# Function to extract confidence interval values
def extract_confidence_values(confidence_interval_str):
    if pd.isna(confidence_interval_str):
        return np.nan, np.nan
    matches = re.findall(r'\d+\.\d+', confidence_interval_str)
    return float(matches[0]), float(matches[1])

# Iterate through each query to generate plots
for query in queries:
    # Filter data for the current query
    data_cassandra_query_first_execution = data_cassandra_first_execution[data_cassandra_first_execution['Query'] == query]
    data_cassandra_query_avg_30 = data_cassandra_avg_30[data_cassandra_avg_30['Query'] == query]
    data_neo4j_query_first_execution = data_neo4j_first_execution[data_neo4j_first_execution['Query'] == query]
    data_neo4j_query_avg_30 = data_neo4j_avg_30[data_neo4j_avg_30['Query'] == query]

    # Create the first plot: First Execution Time
    plt.figure(figsize=(12, 7))
    bar_width = 0.35
    index = np.arange(len(dataset_sizes))

    # Extract execution times for Cassandra and Neo4j
    values_cassandra_first_execution = [data_cassandra_query_first_execution[data_cassandra_query_first_execution['Dataset'] == size]['First Execution Time (ms)'].values[0] for size in dataset_sizes]
    values_neo4j_first_execution = [data_neo4j_query_first_execution[data_neo4j_query_first_execution['Dataset'] == size]['First Execution Time (ms)'].values[0] for size in dataset_sizes]

    # Create bar plots for Cassandra and Neo4j
    plt.bar(index - bar_width/2, values_cassandra_first_execution, bar_width, label='Cassandra', color=color_cassandra)
    plt.bar(index + bar_width/2, values_neo4j_first_execution, bar_width, label='Neo4j', color=color_neo4j)

    plt.xlabel('Dataset Size', fontsize=14)
    plt.ylabel('Execution Time (ms)', fontsize=14)
    plt.title(f'First Execution Time for {query}', fontsize=16, fontweight='bold')
    plt.xticks(index, dataset_sizes, fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(fontsize=12)
    plt.grid(True, which='both', linestyle='--', linewidth=0.5, alpha=0.7)
    plt.tight_layout()

    # Save and show the plot
    filename = f'Histograms/Histogram_Time_Before_Execution_{query}.png'
    plt.savefig(filename, dpi=300)
    plt.show()
    plt.close()

    # Create the second plot: Average Execution Time with Confidence Interval
    plt.figure(figsize=(12, 7))
    values_cassandra_avg_30 = [data_cassandra_query_avg_30[data_cassandra_query_avg_30['Dataset'] == size]['Average of 30 Executions (ms)'].values[0] for size in dataset_sizes]
    values_neo4j_avg_30 = [data_neo4j_query_avg_30[data_neo4j_query_avg_30['Dataset'] == size]['Average of 30 Executions (ms)'].values[0] for size in dataset_sizes]

    # Extract confidence intervals for Cassandra and Neo4j
    conf_intervals_cassandra = [extract_confidence_values(data_cassandra_query_avg_30[data_cassandra_query_avg_30['Dataset'] == size]['Confidence Interval (Min, Max)'].values[0]) for size in dataset_sizes]
    conf_intervals_neo4j = [extract_confidence_values(data_neo4j_query_avg_30[data_neo4j_query_avg_30['Dataset'] == size]['Confidence Interval (Min, Max)'].values[0]) for size in dataset_sizes]

    # Extract minimum and maximum values of confidence intervals
    conf_cassandra_min = [conf[0] for conf in conf_intervals_cassandra]
    conf_cassandra_max = [conf[1] for conf in conf_intervals_cassandra]
    conf_neo4j_min = [conf[0] for conf in conf_intervals_neo4j]
    conf_neo4j_max = [conf[1] for conf in conf_intervals_neo4j]

    # Calculate error bars
    cassandra_yerr = [np.array([values_cassandra_avg_30[i] - conf_cassandra_min[i], conf_cassandra_max[i] - values_cassandra_avg_30[i]]) for i in range(len(dataset_sizes))]
    neo4j_yerr = [np.array([values_neo4j_avg_30[i] - conf_neo4j_min[i], conf_neo4j_max[i] - values_neo4j_avg_30[i]]) for i in range(len(dataset_sizes))]

    # Create bar plots with error bars for Cassandra and Neo4j
    plt.bar(index - bar_width/2, values_cassandra_avg_30, bar_width, yerr=np.array(cassandra_yerr).T, capsize=5, label='Cassandra', color=color_cassandra)
    plt.bar(index + bar_width/2, values_neo4j_avg_30, bar_width, yerr=np.array(neo4j_yerr).T, capsize=5, label='Neo4j', color=color_neo4j)

    plt.xlabel('Dataset Size', fontsize=14)
    plt.ylabel('Average Execution Time (ms)', fontsize=14)
    plt.title(f'Average Execution Time for {query}', fontsize=16, fontweight='bold')
    plt.xticks(index, dataset_sizes, fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(fontsize=12)
    plt.grid(True, which='both', linestyle='--', linewidth=0.5, alpha=0.7)
    plt.tight_layout()

    # Save and show the plot
    filename = f'Histograms/Histogram_Average_Execution_Time_{query}.png'
    plt.savefig(filename, dpi=300)
    plt.show()
    plt.close()
