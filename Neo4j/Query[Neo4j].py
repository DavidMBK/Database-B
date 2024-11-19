from neo4j import GraphDatabase
import time
import csv
import numpy as np
import os
import scipy.stats as stats
import logging

# Configurazione logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

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
        try:
            self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))
            logging.info(f"Connesso a Neo4j database: {database_name}")
        except Exception as e:
            logging.error(f"Errore nella connessione al database: {e}")
            raise

    def close(self):
        if self._driver:
            self._driver.close()
            logging.info(f"Connessione al database chiusa: {self._database_name}")

    def execute_query(self, query, parameters=None):
        try:
            with self._driver.session(database=self._database_name) as session:
                result = session.run(query, parameters)
                return [record for record in result]
        except Exception as e:
            logging.error(f"Errore durante l'esecuzione della query: {query}\nErrore: {e}")
            return []

    def clear_cache(self):
        try:
            self.execute_query("CALL db.clearQueryCaches()")
            logging.info(f"Cache svuotata per il database: {self._database_name}")
        except Exception as e:
            logging.error(f"Errore durante la pulizia della cache: {e}")

def measure_query_time(db, query, warmup=0):
    # Warm-up più lungo
    for _ in range(warmup):
        db.execute_query(query)

    # Misura il tempo effettivo
    start_time = time.perf_counter()
    db.execute_query(query)
    end_time = time.perf_counter()
    return (end_time - start_time) * 1000  # Conversione in millisecondi

def remove_outliers(data):
    if len(data) < 4:
        return data
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
        WHERE v.date >= '2021-01-01' AND rand() < 1
        WITH d, d.specialization AS specialization, COUNT(v) AS total_visits
        RETURN d.id AS doctor_id, specialization, total_visits
        """,
        'Query 2': """ 
        MATCH (v:Visit)-[:INCLUDES_PROCEDURE]-(proc:Procedure)
        WHERE v.date >= '2021-01-01' AND v.date <= '2023-12-31' AND rand() < 1
        WITH proc, proc.description AS description, COUNT(v) AS total_procedures
        RETURN proc.id AS procedure_id, description, total_procedures
        """,
        'Query 3': """
        MATCH (d:Doctor)-[:VISIT_TO]-(v:Visit)-[:VISIT_BY]-(p:Patient)
        WHERE v.date >= '2021-01-01' AND v.date <= '2023-12-31' AND rand() < 1
        WITH d, COUNT(DISTINCT p) AS total_patients
        RETURN d.id AS doctor_id, total_patients
        """,
        'Query 4': """
        MATCH (p:Patient)
        WHERE p.birthdate >= '1940-06-21' AND rand() < 1
        RETURN p
        """
    }

    # Ottieni la directory in cui si trova il file Python
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Crea la cartella 'ResponseTimes' all'interno della stessa directory
    output_directory = os.path.join(current_directory, 'ResponseTimes')
    os.makedirs(output_directory, exist_ok=True)

    for percent, db_name in database_mappings.items():
        db = Neo4jConnection(uri, user, password, db_name)
        
        # Svuota la cache una sola volta prima di cominciare con le query
        db.clear_cache()

        for query_name, query in queries.items():
            response_times = []

            # Warm-up più lungo: esegui la query 10 volte
            measure_query_time(db, query, warmup=10)

            for i in range(101):
                elapsed_time = measure_query_time(db, query, warmup=0)
                response_times.append(elapsed_time)

                if i == 0:
                    first_execution_time = elapsed_time

                logging.info(f"Dataset: {percent} - {query_name} execution {i+1}: {elapsed_time:.2f} ms")
                time.sleep(0.01)

            filtered_response_times = remove_outliers(response_times[1:])

            if filtered_response_times:
                average_time_exact = np.mean(filtered_response_times)
                average_time_rounded = round(average_time_exact, 2)
                std_dev = np.std(filtered_response_times, ddof=1)
                n = len(filtered_response_times)
                confidence_interval = stats.t.interval(0.95, loc=average_time_exact, scale=std_dev / np.sqrt(n), df=n-1)
            else:
                average_time_rounded = average_time_exact = confidence_interval = None

            all_response_times.append({
                'Dataset': percent,
                'Query': query_name,
                'Average of 100 Executions (ms)': f"{average_time_rounded:.2f}" if average_time_rounded is not None else 'N/A',
                'Average Time (ms)': f"{average_time_exact:.6f}" if average_time_exact is not None else 'N/A',
                'Confidence Interval (Min, Max)': f"({confidence_interval[0]:.2f}, {confidence_interval[1]:.2f})" if confidence_interval is not None else 'N/A'
            })

            first_execution_times.append({
                'Dataset': percent,
                'Query': query_name,
                'First Execution Time (ms)': f"{first_execution_time:.2f}"
            })

        db.close()

    # Salvataggio dei dati nei file CSV
    response_times_file = os.path.join(output_directory, 'neo4j_100_avg_execution.csv')
    with open(response_times_file, 'w', newline='') as csvfile:
        fieldnames = ['Dataset', 'Query', 'Average of 100 Executions (ms)', 'Average Time (ms)', 'Confidence Interval (Min, Max)']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_response_times)

    first_execution_times_file = os.path.join(output_directory, 'neo4j_first_execution.csv')
    with open(first_execution_times_file, 'w', newline='') as csvfile:
        fieldnames = ['Dataset', 'Query', 'First Execution Time (ms)']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(first_execution_times)

if __name__ == "__main__":
    process_datasets()
