import time
import csv
from cassandra.cluster import Cluster
import numpy as np
import os
import scipy.stats as stats

# Configurazione della connessione al cluster Cassandra
contact_points = ['127.0.0.1']  # Cambia con il tuo indirizzo del nodo Cassandra
keyspace_mappings = {
    "25%": "healthcare_25",
    "50%": "healthcare_50",
    "75%": "healthcare_75",
    "100%": "healthcare_100"
}

# Creazione della classe per gestire la connessione e le query
class CassandraConnection:
    def __init__(self, contact_points, keyspace):
        self._cluster = Cluster(contact_points)
        self._session = self._cluster.connect()
        self._session.set_keyspace(keyspace)

    def close(self):
        self._cluster.shutdown()

    def execute_query(self, query):
        return self._session.execute(query)

# Funzione per eseguire la query e misurare il tempo di esecuzione
def measure_query_time(session, query):
    start_time = time.perf_counter()  # Uso di perf_counter per maggiore precisione
    session.execute(query)
    end_time = time.perf_counter()
    return (end_time - start_time) * 1000  # Conversione in millisecondi

# Funzione per calcolare l'intervallo di confidenza
def calculate_confidence_interval(data, confidence=0.95):
    n = len(data)
    if n <= 1:
        return np.nan, np.nan  # Non possiamo calcolare l'intervallo con meno di 2 campioni
    average_value = np.mean(data)
    stderr = stats.sem(data)  # Standard Error of the Mean
    margin_of_error = stderr * stats.t.ppf((1 + confidence) / 2, n - 1)
    return average_value, margin_of_error

# Funzione per eseguire la query su tutti i dataset e salvare i risultati nei CSV
def process_datasets():
    all_response_times = []  # Lista per memorizzare i risultati di tutte le query
    first_execution_times = []  # Lista per memorizzare i tempi della prima esecuzione

    # Livello di complessità Non Onerosità
    queries = {
        'Query 1': """
            SELECT *
            FROM patients
            WHERE birthdate >= '1940-06-21'
            ALLOW FILTERING; 
            """,
            #AND visit_date <= '2023-12-31' 
            #GROUP BY patient_id
            #ALLOW FILTERING;
       
        'Query 2': """
            SELECT doctor_id, specialization, SUM(visit_count) AS total_visits 
            FROM doctor_visits 
            WHERE visit_date >= '2021-01-01'
            ALLOW FILTERING;
             """,
            #AND visit_date <= '2023-12-31'
            #GROUP BY doctor_id, specialization
            #ALLOW FILTERING;
       
        'Query 3': """
            SELECT procedure_id, doctor_specialization, 
            SUM(procedure_count) AS total_procedures 
            FROM procedure_visit_stats
            WHERE visit_date >= '2021-01-01' 
            AND visit_date <= '2023-12-31'
            GROUP BY procedure_id, doctor_specialization
            ALLOW FILTERING;
        """,
        'Query 4': """
            SELECT doctor_id, COUNT(*) AS total_patients
            FROM doctor_patient_counts 
            WHERE visit_date >= '2021-01-01'
            AND visit_date <= '2023-12-31'
            GROUP BY doctor_id
            ALLOW FILTERING;
        """
    }

    # Creazione della cartella ResponseTimes all'interno della directory corrente
    output_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ResponseTimes')
    os.makedirs(output_directory, exist_ok=True)

    for percent, keyspace in keyspace_mappings.items():
        db = CassandraConnection(contact_points, keyspace)
        
        for query_name, query in queries.items():
            response_times = []  # Lista per memorizzare i tempi di esecuzione della query

            # Esecuzione della query 31 volte con un breve ritardo tra le esecuzioni
            for i in range(31):
                elapsed_time = measure_query_time(db._session, query)
                if i == 0:
                    # La prima esecuzione viene registrata separatamente
                    first_execution_time = elapsed_time
                else:
                    # Le successive 30 esecuzioni sono registrate per il calcolo della media
                    response_times.append(elapsed_time)
                print(f"Dataset: {percent} - {query_name} execution {i+1}: {elapsed_time:.2f} ms")
                time.sleep(0.001)  # Ritardo di 1 millisecondo tra le esecuzioni

            # Calcolo delle statistiche per le 30 esecuzioni successive
            if response_times:
                average_time_exact = np.mean(response_times)  # Media esatta
                average_time_rounded = round(average_time_exact, 2)  # Media arrotondata
                average, margin_of_error = calculate_confidence_interval(response_times)
                confidence_interval = (average - margin_of_error, average + margin_of_error)
            else:
                average_time_rounded = average = margin_of_error = confidence_interval = (np.nan, np.nan)

            # Aggiungi i risultati alla lista di tutti i risultati
            all_response_times.append({
                'Dataset': percent,
                'Query': query_name,
                'Average of 30 Executions (ms)': f"{average_time_rounded:.2f}" if average_time_rounded is not None else 'N/A',
                'Average Time (ms)': f"{average:.6f}" if average is not None else 'N/A',
                'Confidence Interval (Min, Max)': f"({confidence_interval[0]:.2f}, {confidence_interval[1]:.2f})" if confidence_interval is not None else 'N/A'
            })

            # Scrittura del tempo della prima esecuzione
            first_execution_times.append({
                'Dataset': percent,
                'Query': query_name,
                'First Execution Time (ms)': f"{first_execution_time:.2f}"
            })

        db.close()

    # Scrittura dei risultati nel file CSV cassandra_response_times_average_30.csv
    response_times_file = os.path.join(output_directory, 'cassandra_30_avg_execution.csv')
    with open(response_times_file, 'w', newline='') as csvfile:
        fieldnames = ['Dataset', 'Query', 'Average of 30 Executions (ms)', 'Average Time (ms)', 'Confidence Interval (Min, Max)']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_response_times)

    # Scrittura dei risultati nel file CSV cassandra_times_of_response_first_execution.csv
    first_execution_times_file = os.path.join(output_directory, 'cassandra_first_execution.csv')
    with open(first_execution_times_file, 'w', newline='') as csvfile:
        fieldnames = ['Dataset', 'Query', 'First Execution Time (ms)']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(first_execution_times)

if __name__ == "__main__":
    process_datasets()
