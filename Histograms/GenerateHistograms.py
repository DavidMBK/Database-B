# Importa le librerie necessarie
import pandas as pd
import matplotlib.pyplot as plt
import re
import numpy as np

# Definisce i percorsi ai file CSV per i dati di MongoDB e Neo4j
mongo_csv_paths = [
    "MongoDB/ResponseTimes/mongodb_times_of_response_first_execution.csv",
    "MongoDB/ResponseTimes/mongodb_response_times_average_30.csv",
]

neo4j_csv_paths = [
    "Neo4j/ResponseTimes/neo4j_times_of_response_first_execution.csv",
    "Neo4j/ResponseTimes/neo4j_response_times_average_30.csv",
]

# Carica i dati dai file CSV per MongoDB
data_mongo_first_execution = pd.read_csv(mongo_csv_paths[0], sep=',', dtype={'Confidence Interval (Min, Max)': str})
data_mongo_avg_30 = pd.read_csv(mongo_csv_paths[1], sep=',', dtype={'Confidence Interval (Min, Max)': str})

# Carica i dati dai file CSV per Neo4j
data_neo4j_first_execution = pd.read_csv(neo4j_csv_paths[0], sep=',', dtype={'Confidence Interval (Min, Max)': str})
data_neo4j_avg_30 = pd.read_csv(neo4j_csv_paths[1], sep=',', dtype={'Confidence Interval (Min, Max)': str})

# Definisce le dimensioni del dataset e le query da analizzare
dataset_sizes = ['100%', '75%', '50%', '25%']
queries = ['Query 1', 'Query 2', 'Query 3', 'Query 4']

# Definisce i colori per i grafici
color_mongo = 'coral'
color_neo4j = 'purple'

# Funzione per estrarre i valori di intervallo di confidenza dai dati
def extract_confidence_values(confidence_interval_str):
    if pd.isna(confidence_interval_str):
        return np.nan, np.nan
    matches = re.findall(r'\d+\.\d+', confidence_interval_str)
    return float(matches[0]), float(matches[1])

# Cicla attraverso ogni query per generare i grafici
for query in queries:
    # Filtra i dati per la query corrente
    data_mongo_query_first_execution = data_mongo_first_execution[data_mongo_first_execution['Query'] == query]
    data_mongo_query_avg_30 = data_mongo_avg_30[data_mongo_avg_30['Query'] == query]
    data_neo4j_query_first_execution = data_neo4j_first_execution[data_neo4j_first_execution['Query'] == query]
    data_neo4j_query_avg_30 = data_neo4j_avg_30[data_neo4j_avg_30['Query'] == query]

    # Crea il primo grafico: Tempo di esecuzione per la prima esecuzione
    plt.figure(figsize=(12, 7))
    bar_width = 0.35
    index = np.arange(len(dataset_sizes))

    # Estrai i valori di tempo di esecuzione per MongoDB e Neo4j
    values_mongo_first_execution = [data_mongo_query_first_execution[data_mongo_query_first_execution['Dataset'] == size]['Milliseconds'].values[0] for size in dataset_sizes]
    values_neo4j_first_execution = [data_neo4j_query_first_execution[data_neo4j_query_first_execution['Dataset'] == size]['Milliseconds'].values[0] for size in dataset_sizes]

    # Crea i barplot per MongoDB e Neo4j
    plt.bar(index - bar_width/2, values_mongo_first_execution, bar_width, label='MongoDB', color=color_mongo)
    plt.bar(index + bar_width/2, values_neo4j_first_execution, bar_width, label='Neo4j', color=color_neo4j)

    plt.xlabel('Dataset Size')
    plt.ylabel('Execution Time (ms)')
    plt.title(f'Histogram - First Execution Time for {query}')
    plt.xticks(index, dataset_sizes)
    plt.legend()
    plt.tight_layout()

    # Salva e mostra il grafico
    filename = f'Histograms/Histogram_Time_Before_Execution_{query}.png'
    plt.savefig(filename)
    plt.show()
    plt.close()

    # Crea il secondo grafico: Tempo di esecuzione medio con intervallo di confidenza
    plt.figure(figsize=(12, 7))
    values_mongo_avg_30 = [data_mongo_query_avg_30[data_mongo_query_avg_30['Dataset'] == size]['Average'].values[0] for size in dataset_sizes]
    values_neo4j_avg_30 = [data_neo4j_query_avg_30[data_neo4j_query_avg_30['Dataset'] == size]['Average'].values[0] for size in dataset_sizes]

    # Estrai gli intervalli di confidenza per MongoDB e Neo4j
    conf_intervals_mongo = [extract_confidence_values(data_mongo_query_avg_30[data_mongo_query_avg_30['Dataset'] == size]['Confidence Interval (Min, Max)'].values[0]) for size in dataset_sizes]
    conf_intervals_neo4j = [extract_confidence_values(data_neo4j_query_avg_30[data_neo4j_query_avg_30['Dataset'] == size]['Confidence Interval (Min, Max)'].values[0]) for size in dataset_sizes]

    # Estrai i valori minimi e massimi degli intervalli di confidenza
    conf_mongo_min = [conf[0] for conf in conf_intervals_mongo]
    conf_mongo_max = [conf[1] for conf in conf_intervals_mongo]
    conf_neo4j_min = [conf[0] for conf in conf_intervals_neo4j]
    conf_neo4j_max = [conf[1] for conf in conf_intervals_neo4j]

    # Calcola gli errori per le barre di errore
    mongo_yerr = [np.array([values_mongo_avg_30[i] - conf_mongo_min[i], conf_mongo_max[i] - values_mongo_avg_30[i]]) for i in range(len(dataset_sizes))]
    neo4j_yerr = [np.array([values_neo4j_avg_30[i] - conf_neo4j_min[i], conf_neo4j_max[i] - values_neo4j_avg_30[i]]) for i in range(len(dataset_sizes))]

    # Crea i barplot con barre di errore per MongoDB e Neo4j
    plt.bar(index - bar_width/2, values_mongo_avg_30, bar_width, yerr=np.array(mongo_yerr).T, capsize=5, label='MongoDB', color=color_mongo)
    plt.bar(index + bar_width/2, values_neo4j_avg_30, bar_width, yerr=np.array(neo4j_yerr).T, capsize=5, label='Neo4j', color=color_neo4j)

    plt.xlabel('Dataset Size')
    plt.ylabel('Average Execution Time (ms)')
    plt.title(f'Histogram - Average Execution Time for {query}')
    plt.xticks(index, dataset_sizes)
    plt.legend()
    plt.tight_layout()

    # Salva e mostra il grafico
    filename = f'Histograms/Histogram_Average_Execution_Time_{query}.png'
    plt.savefig(filename)
    plt.show()
    plt.close()
