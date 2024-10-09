import pandas as pd
import matplotlib.pyplot as plt
import re
import numpy as np
import seaborn as sns
import os

# Imposta lo stile di seaborn per i grafici
sns.set(style="whitegrid")

# Definisci i percorsi dei file CSV per i dati di Cassandra e Neo4j
cassandra_csv_path = [
    "Cassandra/ResponseTimes/cassandra_first_execution.csv",
    "Cassandra/ResponseTimes/cassandra_100_avg_execution.csv",  # Modificato a 100 esecuzioni
]

neo4j_csv_paths = [
    "Neo4j/ResponseTimes/neo4j_first_execution.csv",
    "Neo4j/ResponseTimes/neo4j_100_avg_execution.csv",  # Modificato a 100 esecuzioni
]

# Controlla se i file esistono
for path in cassandra_csv_path + neo4j_csv_paths:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"The file {path} does not exist.")

# Carica i dati dai file CSV
data_cassandra_first_execution = pd.read_csv(cassandra_csv_path[0], sep=',')
data_cassandra_avg_100 = pd.read_csv(cassandra_csv_path[1], sep=',')  # Modificato a 100 esecuzioni
data_neo4j_first_execution = pd.read_csv(neo4j_csv_paths[0], sep=',')
data_neo4j_avg_100 = pd.read_csv(neo4j_csv_paths[1], sep=',')  # Modificato a 100 esecuzioni

# Definisci le dimensioni del dataset e le query da analizzare
dataset_sizes = ['25%', '50%', '75%', '100%']
queries = ['Query 1', 'Query 2', 'Query 3', 'Query 4']

# Definisci i colori per i grafici
color_cassandra = '#2b93db'  # Blu vivace
color_neo4j = '#4bce4b'     # Lime brillante

# Funzione per estrarre i valori dell'intervallo di confidenza
def extract_confidence_values(confidence_interval_str):
    if pd.isna(confidence_interval_str):
        return np.nan, np.nan
    matches = re.findall(r'\d+\.\d+', confidence_interval_str)
    return float(matches[0]), float(matches[1])

# Itera attraverso ogni query per generare i grafici
for query in queries:
    # Filtra i dati per la query corrente
    data_cassandra_query_first_execution = data_cassandra_first_execution[data_cassandra_first_execution['Query'] == query]
    data_cassandra_query_avg_100 = data_cassandra_avg_100[data_cassandra_avg_100['Query'] == query]
    data_neo4j_query_first_execution = data_neo4j_first_execution[data_neo4j_first_execution['Query'] == query]
    data_neo4j_query_avg_100 = data_neo4j_avg_100[data_neo4j_avg_100['Query'] == query]

    # Crea il primo grafico: Tempo di Prima Esecuzione
    plt.figure(figsize=(12, 7))
    bar_width = 0.35
    index = np.arange(len(dataset_sizes))

    # Estrai i tempi di esecuzione per Cassandra e Neo4j
    values_cassandra_first_execution = [
        data_cassandra_query_first_execution[data_cassandra_query_first_execution['Dataset'] == size]['First Execution Time (ms)'].values[0] 
        for size in dataset_sizes
    ]
    values_neo4j_first_execution = [
        data_neo4j_query_first_execution[data_neo4j_query_first_execution['Dataset'] == size]['First Execution Time (ms)'].values[0] 
        for size in dataset_sizes
    ]

    # Crea i grafici a barre per Cassandra e Neo4j
    plt.bar(index - bar_width/2, values_cassandra_first_execution, bar_width, label='Cassandra', color=color_cassandra)
    plt.bar(index + bar_width/2, values_neo4j_first_execution, bar_width, label='Neo4j', color=color_neo4j)

    plt.xlabel('Dimensione del Dataset', fontsize=14)
    plt.ylabel('Tempo di Esecuzione (ms)', fontsize=14)
    plt.title(f'Tempo di Prima Esecuzione per {query}', fontsize=16, fontweight='bold')
    plt.xticks(index, dataset_sizes, fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(fontsize=12)
    plt.grid(True, which='both', linestyle='--', linewidth=0.5, alpha=0.7)
    plt.tight_layout()

    # Salva e mostra il grafico
    filename = f'Histograms/Histogram_First_ExecutionTime_{query}.png'
    plt.savefig(filename, dpi=300)
    plt.show()
    plt.close()

    # Crea il secondo grafico: Tempo Medio di Esecuzione con Intervallo di Confidenza
    plt.figure(figsize=(12, 7))
    values_cassandra_avg_100 = [
        data_cassandra_query_avg_100[data_cassandra_query_avg_100['Dataset'] == size]['Average of 100 Executions (ms)'].values[0] 
        for size in dataset_sizes
    ]
    values_neo4j_avg_100 = [
        data_neo4j_query_avg_100[data_neo4j_query_avg_100['Dataset'] == size]['Average of 100 Executions (ms)'].values[0] 
        for size in dataset_sizes
    ]

    # Estrai gli intervalli di confidenza per Cassandra e Neo4j
    conf_intervals_cassandra = [
        extract_confidence_values(data_cassandra_query_avg_100[data_cassandra_query_avg_100['Dataset'] == size]['Confidence Interval (Min, Max)'].values[0]) 
        for size in dataset_sizes
    ]
    conf_intervals_neo4j = [
        extract_confidence_values(data_neo4j_query_avg_100[data_neo4j_query_avg_100['Dataset'] == size]['Confidence Interval (Min, Max)'].values[0]) 
        for size in dataset_sizes
    ]

    # Estrai i valori minimi e massimi degli intervalli di confidenza
    conf_cassandra_min = [conf[0] for conf in conf_intervals_cassandra]
    conf_cassandra_max = [conf[1] for conf in conf_intervals_cassandra]
    conf_neo4j_min = [conf[0] for conf in conf_intervals_neo4j]
    conf_neo4j_max = [conf[1] for conf in conf_intervals_neo4j]

    # Calcola le barre di errore
    cassandra_yerr = [np.array([values_cassandra_avg_100[i] - conf_cassandra_min[i], conf_cassandra_max[i] - values_cassandra_avg_100[i]]) for i in range(len(dataset_sizes))]
    neo4j_yerr = [np.array([values_neo4j_avg_100[i] - conf_neo4j_min[i], conf_neo4j_max[i] - values_neo4j_avg_100[i]]) for i in range(len(dataset_sizes))]

    # Crea i grafici a barre con barre di errore per Cassandra e Neo4j
    plt.bar(index - bar_width/2, values_cassandra_avg_100, bar_width, yerr=np.array(cassandra_yerr).T, capsize=5, label='Cassandra', color=color_cassandra)
    plt.bar(index + bar_width/2, values_neo4j_avg_100, bar_width, yerr=np.array(neo4j_yerr).T, capsize=5, label='Neo4j', color=color_neo4j)

    plt.xlabel('Dimensione del Dataset', fontsize=14)
    plt.ylabel('Tempo Medio di Esecuzione (ms)', fontsize=14)
    plt.title(f'Tempo Medio di Esecuzione per {query}', fontsize=16, fontweight='bold')
    plt.xticks(index, dataset_sizes, fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(fontsize=12)
    plt.grid(True, which='both', linestyle='--', linewidth=0.5, alpha=0.7)
    plt.tight_layout()

    # Salva e mostra il grafico
    filename = f'Histograms/Histogram_100_Avg_ExecutionTime_{query}.png'  # Modificato a 100 esecuzioni
    plt.savefig(filename, dpi=300)
    plt.show()
    plt.close()
