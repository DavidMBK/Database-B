from neo4j import GraphDatabase
import time
import csv
import numpy as np
import os
import scipy.stats as stats

# Configurazione della connessione al database
uri = "bolt://localhost:7687"
user = "neo4j"
password = "12345678"
database_mappings = {
    "100%": "dataset100",
    "75%": "dataset75",
    "50%": "dataset50",
    "25%": "dataset25"
}

class Neo4jConnection:
    def __init__(self, uri, user, password, database_name):
        self._uri = uri
        self._user = user
        self._password = password
        self._database_name = database_name
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))

    def close(self):
        self._driver.close()

    def execute_query(self, query, parameters=None):
        with self._driver.session(database=self._database_name) as session:
            result = session.run(query, parameters)
            return [record for record in result]

def measure_query_time(db, query):
    start_time = time.perf_counter()
    db.execute_query(query)
    end_time = time.perf_counter()
    return (end_time - start_time) * 1000  # Conversione in millisecondi

def remove_outliers(data):
    if len(data) < 4:
        return data  # Non rimuovere outliers se ci sono meno di 4 dati
    q1 = np.percentile(data, 25)
    q3 = np.percentile(data, 75)
    iqr = q3 - q1
    return [x for x in data if (x >= (q1 - 1.5 * iqr)) and (x <= (q3 + 1.5 * iqr))]

def process_datasets():
    all_response_times = []
    first_execution_times = []

    queries = {
        'Query 1': """
        MATCH (d:Doctor)-[:VISIT_TO]-(v:Visit)
        WHERE v.date >= '2021-01-01'
        WITH d, d.specialization AS specialization, COUNT(v) AS total_visits
        RETURN d.id AS doctor_id, specialization, total_visits
        """,
        'Query 2': """ 
        MATCH (v:Visit)-[:INCLUDES_PROCEDURE]-(proc:Procedure)
        WHERE v.date >= '2021-01-01' AND v.date <= '2023-12-31'
        WITH proc, proc.description AS description, COUNT(v) AS total_procedures
        RETURN proc.id AS procedure_id, description, total_procedures
        """,
        'Query 3': """
        MATCH (d:Doctor)-[:VISIT_TO]-(v:Visit)-[:VISIT_BY]-(p:Patient)
        WHERE v.date >= '2021-01-01' AND v.date <= '2023-12-31'
        WITH d, COUNT(DISTINCT p) AS total_patients
        RETURN d.id AS doctor_id, total_patients
        """,
        'Query 4': """
        MATCH (p:Patient)
        WHERE p.birthdate >= '1940-06-21'
        RETURN p
        """
    }

    output_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ResponseTimes')
    os.makedirs(output_directory, exist_ok=True)

    for percent, db_name in database_mappings.items():
        db = Neo4jConnection(uri, user, password, db_name)

        for query_name, query in queries.items():
            response_times = []

            # Esecuzione della query 101 volte per mantenere la prima esecuzione
            for i in range(101):
                elapsed_time = measure_query_time(db, query)
                response_times.append(elapsed_time)

                if i == 0:
                    first_execution_time = elapsed_time  # Salva il tempo della prima esecuzione
                
                print(f"Dataset: {percent} - {query_name} execution {i+1}: {elapsed_time:.2f} ms")
                time.sleep(0.01)  # Ritardo di 10 millisecondi tra le esecuzioni

            # Rimuovere i valori anomali
            filtered_response_times = remove_outliers(response_times[1:])  # Rimuovi outlier solo dalle 100 esecuzioni successive

            # Calcolo delle statistiche
            if filtered_response_times:
                average_time_exact = np.mean(filtered_response_times)
                average_time_rounded = round(average_time_exact, 2)
                std_dev = np.std(filtered_response_times, ddof=1)
                n = len(filtered_response_times)
                confidence_interval = stats.norm.interval(0.95, loc=average_time_exact, scale=std_dev/np.sqrt(n))
            else:
                average_time_rounded = average_time_exact = confidence_interval = None

            all_response_times.append({
                'Dataset': percent,
                'Query': query_name,
                'Average of 100 Executions (ms)': f"{average_time_rounded:.2f}" if average_time_rounded is not None else 'N/A',
                'Average Time (ms)': f"{average_time_exact:.6f}" if average_time_exact is not None else 'N/A',
                'Confidence Interval (Min, Max)': f"({confidence_interval[0]:.2f}, {confidence_interval[1]:.2f})" if confidence_interval is not None else 'N/A'
            })

            # Salva il tempo della prima esecuzione
            first_execution_times.append({
                'Dataset': percent,
                'Query': query_name,
                'First Execution Time (ms)': f"{first_execution_time:.2f}"
            })

        # Chiusura della connessione
        db.close()

    # Scrittura dei risultati nel file CSV
    response_times_file = os.path.join(output_directory, 'neo4j_100_avg_execution.csv')
    with open(response_times_file, 'w', newline='') as csvfile:
        fieldnames = ['Dataset', 'Query', 'Average of 100 Executions (ms)', 'Average Time (ms)', 'Confidence Interval (Min, Max)']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_response_times)

    # Scrittura dei risultati nel file CSV per la prima esecuzione
    first_execution_times_file = os.path.join(output_directory, 'neo4j_first_execution.csv')
    with open(first_execution_times_file, 'w', newline='') as csvfile:
        fieldnames = ['Dataset', 'Query', 'First Execution Time (ms)']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(first_execution_times)

if __name__ == "__main__":
    process_datasets()
