import json
import time
import csv
from cassandra.cluster import Cluster
from uuid import UUID
import numpy as np
import scipy.stats as stats
from cassandra.query import dict_factory
from datetime import datetime

# Funzione per convertire in modo sicuro i dati in JSON
def safe_serialize(obj):
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()  # Converte la data in una stringa ISO
    raise TypeError(f"Type {type(obj)} not serializable")

# Funzione per calcolare l'intervallo di confidenza
def calculate_confidence_interval(data, confidence=0.95):
    n = len(data)
    average_value = np.mean(data)
    stderr = stats.sem(data)
    margin_of_error = stderr * stats.t.ppf((1 + confidence) / 2, n - 1)
    return average_value, margin_of_error

# Funzione per misurare le performance di una query
def measure_query_performance(session, query_func, iterations=30):
    subsequent_times = []
    
    for _ in range(iterations):
        start_time = time.time()
        query_func(session)
        end_time = time.time()
        execution_time = (end_time - start_time) * 1000  # Tempo in millisecondi
        subsequent_times.append(execution_time)
    
    average, margin_of_error = calculate_confidence_interval(subsequent_times)
    average_subsequent_time = round(np.mean(subsequent_times), 2)
    
    return average_subsequent_time, average, margin_of_error

# Connessione al cluster Cassandra
def create_session(contact_points, keyspace):
    cluster = Cluster(contact_points)
    session = cluster.connect()
    session.set_keyspace(keyspace)
    session.row_factory = dict_factory  # Restituisce i risultati come dizionari
    return session

# Definizione delle query
def query1(session):
    query = "SELECT * FROM patient_visits;"
    return session.execute(query)

def query2(session):
    query = "SELECT * FROM patient_visits WHERE doctor_name >= 'N' AND doctor_name < 'Z' ALLOW FILTERING;"
    return session.execute(query)

def query3(session):
    query = "SELECT * FROM patient_visits WHERE visit_date >= '2021-01-01' AND procedure_name = 'Reactive 24/7 productivity' ALLOW FILTERING;"
    return session.execute(query)

def query4(session):
    query = """
    SELECT * FROM patient_visits
    WHERE visit_date >= '2021-01-01' AND procedure_name = 'Reverse-engineered homogeneous standardization' AND visit_duration > 60
    ALLOW FILTERING;
    """
    return session.execute(query)

def main():
    # Configurazione della connessione
    contact_points = ['127.0.0.1']  # Cambia con il tuo indirizzo del nodo Cassandra
    keyspaces = {
        '25%': 'healthcare_25',
        '50%': 'healthcare_50',
        '75%': 'healthcare_75',
        '100%': 'healthcare_100'
    }
    
    response_times_first_execution = {}
    average_response_times = {}

    for percentage, keyspace in keyspaces.items():
        print(f"\nAnalysis for keyspace: {keyspace}\n")
        
        # Crea sessione per il keyspace corrente
        session = create_session(contact_points, keyspace)

        # Query 1
        try:
            start_time = time.time()
            query_result = query1(session)
            json_result = json.dumps([dict(row) for row in query_result], indent=4, default=safe_serialize)
            print(f"Query 1 Result: \n{json_result}\n")
            end_time = time.time()
            time_first_execution = round((end_time - start_time) * 1000, 2)
            print(f"Response time (first execution - Query 1): {time_first_execution} ms")
            response_times_first_execution[f"{percentage} - Query 1"] = time_first_execution

            average_subsequent_time, average, margin_of_error = measure_query_performance(session, query1)
            print(f"Average time of 30 subsequent executions (Query 1): {average_subsequent_time} ms")
            print(f"Confidence Interval (Query 1): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
            average_response_times[f"{percentage} - Query 1"] = (average_subsequent_time, average, margin_of_error)
        except Exception as e:
            print(f"Error executing Query 1: {e}")

        # Query 2
        try:
            start_time = time.time()
            query_result = query2(session)
            json_result = json.dumps([dict(row) for row in query_result], indent=4, default=safe_serialize)
            print(f"Query 2 Result: \n{json_result}\n")
            end_time = time.time()
            time_first_execution = round((end_time - start_time) * 1000, 2)
            print(f"Response time (first execution - Query 2): {time_first_execution} ms")
            response_times_first_execution[f"{percentage} - Query 2"] = time_first_execution

            average_subsequent_time, average, margin_of_error = measure_query_performance(session, query2)
            print(f"Average time of 30 subsequent executions (Query 2): {average_subsequent_time} ms")
            print(f"Confidence Interval (Query 2): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
            average_response_times[f"{percentage} - Query 2"] = (average_subsequent_time, average, margin_of_error)
        except Exception as e:
            print(f"Error executing Query 2: {e}")

        # Query 3
        try:
            start_time = time.time()
            query_result = query3(session)
            json_result = json.dumps([dict(row) for row in query_result], indent=4, default=safe_serialize)
            print(f"Query 3 Result: \n{json_result}\n")
            end_time = time.time()
            time_first_execution = round((end_time - start_time) * 1000, 2)
            print(f"Response time (first execution - Query 3): {time_first_execution} ms")
            response_times_first_execution[f"{percentage} - Query 3"] = time_first_execution

            average_subsequent_time, average, margin_of_error = measure_query_performance(session, query3)
            print(f"Average time of 30 subsequent executions (Query 3): {average_subsequent_time} ms")
            print(f"Confidence Interval (Query 3): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
            average_response_times[f"{percentage} - Query 3"] = (average_subsequent_time, average, margin_of_error)
        except Exception as e:
            print(f"Error executing Query 3: {e}")

        # Query 4
        try:
            start_time = time.time()
            query_result = query4(session)
            json_result = json.dumps([dict(row) for row in query_result], indent=4, default=safe_serialize)
            print(f"Query 4 Result: \n{json_result}\n")
            end_time = time.time()
            time_first_execution = round((end_time - start_time) * 1000, 2)
            print(f"Response time (first execution - Query 4): {time_first_execution} ms")
            response_times_first_execution[f"{percentage} - Query 4"] = time_first_execution

            average_subsequent_time, average, margin_of_error = measure_query_performance(session, query4)
            print(f"Average time of 30 subsequent executions (Query 4): {average_subsequent_time} ms")
            print(f"Confidence Interval (Query 4): [{round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)}] ms\n")
            average_response_times[f"{percentage} - Query 4"] = (average_subsequent_time, average, margin_of_error)
        except Exception as e:
            print(f"Error executing Query 4: {e}")

    # Salvataggio dei tempi di risposta della prima esecuzione in un file CSV
    with open('Cassandra/Analysis/cassandra_times_of_response_first_execution.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Dataset', 'Query', 'Milliseconds'])
        for key, value in response_times_first_execution.items():
            dataset, query = key.split(' - ')
            writer.writerow([dataset, query, value])

    # Salvataggio dei tempi di risposta medi in un file CSV
    with open('Cassandra/Analysis/cassandra_average_30_response_times.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Dataset', 'Query', 'Milliseconds', 'Average', 'Confidence Interval (Min, Max)'])
        for key, value in average_response_times.items():
            dataset, query = key.split(' - ')
            average_subsequent_time, average, margin_of_error = value
            writer.writerow([dataset, query, average_subsequent_time, average, f"({round(average - margin_of_error, 2)}, {round(average + margin_of_error, 2)})"])

if __name__ == "__main__":
    main()
