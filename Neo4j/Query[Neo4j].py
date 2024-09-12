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
    "25%": "dataset25",
    "50%": "dataset50",
    "75%": "dataset75",
    "100%": "dataset100"
}

# Creazione della classe per gestire la connessione e le query
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

# Funzione per eseguire la query e misurare il tempo di esecuzione
def measure_query_time(db, query):
    start_time = time.perf_counter()
    db.execute_query(query)
    end_time = time.perf_counter()
    return (end_time - start_time) * 1000  # Conversione in millisecondi

# Funzione per eseguire la query su tutti i dataset e salvare i risultati nei CSV
def process_datasets():
    all_response_times = []
    first_execution_times = []

    queries = {
        'Query 1': """
        MATCH (p:Patient)-[:VISIT_BY]-(v:Visit)
        WHERE v.date >= '2021-01-01' AND v.date <= '2023-12-31'
        WITH p, COUNT(v) AS total_visits
        RETURN p.id AS patient_id, total_visits
        """,
        'Query 2': """ 
        MATCH (d:Doctor)-[:VISIT_TO]-(v:Visit)
        WHERE v.date >= '2021-01-01' AND v.date <= '2023-12-31'
        WITH d, d.specialization AS specialization, COUNT(v) AS total_visits
        RETURN d.id AS doctor_id, specialization, total_visits
        """,
        'Query 3': """
        MATCH (v:Visit)-[:INCLUDES_PROCEDURE]-(proc:Procedure)
        WHERE v.date >= '2021-01-01' AND v.date <= '2023-12-31'
        WITH proc, proc.description AS description, COUNT(v) AS total_procedures
        RETURN proc.id AS procedure_id, description, total_procedures
        """,
        'Query 4': """
        MATCH (d:Doctor)-[:VISIT_TO]-(v:Visit)-[:VISIT_BY]-(p:Patient)
        WHERE v.date >= '2021-01-01' AND v.date <= '2023-12-31'
        WITH d, COUNT(DISTINCT p) AS total_patients
        RETURN d.id AS doctor_id, total_patients
        """
    }

    # Creazione della cartella ResponseTimes all'interno della directory corrente
    output_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ResponseTimes')
    os.makedirs(output_directory, exist_ok=True)

    for percent, db_name in database_mappings.items():
        db = Neo4jConnection(uri, user, password, db_name)
        
        for query_name, query in queries.items():
            response_times = []

            # Esecuzione della query 31 volte con un breve ritardo tra le esecuzioni
            for i in range(31):
                elapsed_time = measure_query_time(db, query)
                if i == 0:
                    first_execution_time = elapsed_time
                else:
                    response_times.append(elapsed_time)
                print(f"Dataset: {percent} - {query_name} execution {i+1}: {elapsed_time:.2f} ms")
                time.sleep(0.001)  # Ritardo di 1 millisecondo tra le esecuzioni

            # Calcolo delle statistiche per le 30 esecuzioni successive
            if response_times:
                average_time_exact = np.mean(response_times)
                average_time_rounded = round(average_time_exact, 2)
                std_dev = np.std(response_times, ddof=1)
                n = len(response_times)
                confidence_interval = stats.norm.interval(0.95, loc=average_time_exact, scale=std_dev/np.sqrt(n))
            else:
                average_time_rounded = average_time_exact = confidence_interval = None

            # Aggiungi i risultati alla lista di tutti i risultati
            all_response_times.append({
                'Dataset': percent,
                'Query': query_name,
                'Average of 30 Executions (ms)': f"{average_time_rounded:.2f}" if average_time_rounded is not None else 'N/A',
                'Average Time (ms)': f"{average_time_exact:.6f}" if average_time_exact is not None else 'N/A',
                'Confidence Interval (Min, Max)': f"({confidence_interval[0]:.2f}, {confidence_interval[1]:.2f})" if confidence_interval is not None else 'N/A'
            })

            # Scrittura del tempo della prima esecuzione
            first_execution_times.append({
                'Dataset': percent,
                'Query': query_name,
                'First Execution Time (ms)': f"{first_execution_time:.2f}"
            })

        # Chiusura della connessione
        db.close()

    # Scrittura dei risultati nel file CSV neo4j_response_times_average_30.csv
    response_times_file = os.path.join(output_directory, 'neo4j_30_avg_execution.csv')
    with open(response_times_file, 'w', newline='') as csvfile:
        fieldnames = ['Dataset', 'Query', 'Average of 30 Executions (ms)', 'Average Time (ms)', 'Confidence Interval (Min, Max)']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_response_times)

    # Scrittura dei risultati nel file CSV neo4j_times_of_response_first_execution.csv
    first_execution_times_file = os.path.join(output_directory, 'neo4j_first_execution.csv')
    with open(first_execution_times_file, 'w', newline='') as csvfile:
        fieldnames = ['Dataset', 'Query', 'First Execution Time (ms)']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(first_execution_times)

if __name__ == "__main__":
    process_datasets()
