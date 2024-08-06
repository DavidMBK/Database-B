from datetime import datetime
import json
from bson import ObjectId
from pymongo import MongoClient
import time
import csv
import scipy.stats as stats
import numpy as np

# Funzione per serializzare oggetti ObjectId in formato stringa per JSON
def json_serializer(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError("Non-serializable type")

# Funzione per convertire i campi stringa in oggetti JSON, se possibile
def convert_string_fields_to_json(doc):
    if isinstance(doc, dict):
        for key, value in doc.items():
            if isinstance(value, str):
                try:
                    # Prova a convertire la stringa in un oggetto JSON
                    doc[key] = json.loads(value)
                except json.JSONDecodeError:
                    # Se non è una stringa JSON valida, mantiene il valore originale
                    pass
    return doc

# Funzione per calcolare l'intervallo di confidenza per una media
def calculate_confidence_interval(data, confidence=0.95):
    n = len(data)  # Numero di osservazioni
    mean_value = np.mean(data)  # Media dei dati
    stderr = stats.sem(data)  # Errore standard della media
    margin_of_error = stderr * stats.t.ppf((1 + confidence) / 2, n - 1)  # Margine d'errore
    return mean_value, margin_of_error

# Funzione per misurare la performance delle query
def measure_query_performance(db, query_number, percentage, iterations=30):
    subsequent_times = []  # Lista per memorizzare i tempi di esecuzione
    
    for _ in range(iterations):
        start_time = time.time()  # Inizio misurazione tempo
        if query_number == 1:
            query1(db, percentage)
        elif query_number == 2:
            query2(db, percentage)
        elif query_number == 3:
            query3(db, percentage)
        elif query_number == 4:
            query4(db, percentage)
        end_time = time.time()  # Fine misurazione tempo
        execution_time = (end_time - start_time) * 1000  # Tempo di esecuzione in millisecondi
        subsequent_times.append(execution_time)
    
    # Calcola il tempo medio e l'intervallo di confidenza
    mean, margin_of_error = calculate_confidence_interval(subsequent_times)
    average_time_subsequent = round(sum(subsequent_times) / len(subsequent_times), 2)
    
    return average_time_subsequent, mean, margin_of_error

# Query 1: Recupera un'azienda specifica in base al nome
def query1(db, percentage):
    company_name = 'Sutton-Nolan'
    companies = f'Companies {percentage}'  # Nome della collezione per il dataset
    query = db[companies].find_one({"name": company_name})  # Esegui la query

    return company_name, query

# Query 2: Recupera i dettagli di un'azienda e dei suoi amministratori
def query2(db, percentage):
    company_id = 200
    companies = f'Companies {percentage}'
    administrators = f'Administrators {percentage}'
    company = db[companies].find_one({"id": company_id})
    
    if not company:
        return company_id, False, []

    company = convert_string_fields_to_json(company)
    
    # Recupera i dettagli degli amministratori
    admin_ids = company.get('administrators', [])
    administrators_details = list(db[administrators].find({"id": {"$in": admin_ids}}))

    # Aggiungi i dettagli degli amministratori all'azienda
    company['administrators_details'] = administrators_details

    return company_id, company, administrators_details

# Versione alternativa della Query 2 (commentata)
""" def query2(db, percentage):
    company_id = 4
    companies = f'Companies {percentage}'
    administrators = f'Administrators {percentage}'
    company = db[companies].find_one({"id": company_id})
    
    if company:
        company = convert_string_fields_to_json(company)
        if 'administrators' in company and isinstance(company['administrators'], list):
            administrators = list(db[administrators].find({"id": {"$in": company['administrators']}}))
            company['administrators_details'] = administrators
        else:
            administrators = []
            company['administrators_details'] = []
    else:
        return company_id, False, []

    return company_id, company, administrators """

# Query 3: Recupera i dettagli di un'azienda, dei suoi amministratori e degli UBO con più del 25% di proprietà
def query3(db, percentage):
    companies_collection = f'Companies {percentage}'
    administrators_collection = f'Administrators {percentage}'
    ubos_collection = f'UBO {percentage}'
    company_id = 4550  # Modifica questo ID in base ai tuoi dati

    company = db[companies_collection].find_one({"id": company_id})

    if not company:
        return company_id, False, [], []

    company = convert_string_fields_to_json(company)
    
    # Recupera i dettagli degli amministratori
    admin_ids = company.get('administrators', [])
    administrators_details = list(db[administrators_collection].find({"id": {"$in": admin_ids}}))

    # Recupera i dettagli degli UBO
    ubo_ids = company.get('ubo', [])
    ubos_details = list(db[ubos_collection].find({"id": {"$in": ubo_ids}, "ownership_percentage": {"$gt": 25}}))

    # Aggiungi i dettagli degli amministratori e degli UBO all'azienda
    company['administrators_details'] = administrators_details
    company['ubo_details'] = ubos_details

    return company_id, company, administrators_details, ubos_details

# Query 4: Recupera i dettagli di un'azienda, dei suoi amministratori, degli UBO con più del 25% di proprietà, e la somma delle transazioni in un periodo
def query4(db, percentage):
    companies_collection = f'Companies {percentage}'
    administrators_collection = f'Administrators {percentage}'
    ubos_collection = f'UBO {percentage}'
    transactions_collection = f'Transactions {percentage}'
    
    company_id = 1  # Modifica questo ID in base ai tuoi dati

    # Recupera l'azienda
    company = db[companies_collection].find_one({"id": company_id})
    
    if not company:
        return company_id, False, [], [], 0
    
    company = convert_string_fields_to_json(company)
    
    # Recupera i dettagli degli amministratori
    admin_ids = company.get('administrators', [])
    administrators_details = list(db[administrators_collection].find({"id": {"$in": admin_ids}}))

    # Recupera i dettagli degli UBO
    ubo_ids = company.get('ubo', [])
    ubos_details = list(db[ubos_collection].find({"id": {"$in": ubo_ids}, "ownership_percentage": {"$gt": 25}}))

    # Recupera la somma delle transazioni in un periodo specifico
    transaction_ids = company.get('transactions', [])
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2022, 12, 31)
    
    transaction_summary = db[transactions_collection].aggregate([
        {"$match": {"id": {"$in": transaction_ids}, "date": {"$gte": start_date, "$lte": end_date}}},
        {"$group": {"_id": None, "total_amount": {"$sum": "$amount"}}}
    ])

    # Estrai il totale delle transazioni
    total_amount = 0
    for record in transaction_summary:
        total_amount = record.get('total_amount', 0)
    
    # Aggiungi i dettagli degli amministratori, degli UBO e delle transazioni all'azienda
    company['administrators_details'] = administrators_details
    company['ubo_details'] = ubos_details
    company['transactions_summary'] = total_amount
    
    return company_id, company, administrators_details, ubos_details, total_amount

# Funzione principale per eseguire le query e misurare le performance
def main():
    # Crea una connessione al server MongoDB locale
    client = MongoClient("mongodb://localhost:27017/")
    db = client['UBO']  # Seleziona il database 'UBO'
    
    percentages = ['100%', '75%', '50%', '25%']  # Percentuali del dataset

    first_execution_response_times = {}  # Dizionario per i tempi di risposta della prima esecuzione
    average_response_times = {}  # Dizionario per i tempi di risposta medi

    for percentage in percentages:
        print(f"\nAnalysis by percentage: {percentage}\n")

        # Query 1: Recupera il nome dell'azienda con il nome specificato
        start_time = time.time()
        company_name, query = query1(db, percentage)
        if query:
            query_json = convert_string_fields_to_json(query)
            json_result = json.dumps(query_json, indent=4, default=json_serializer)
            print(f"Company name with the specified name: \n{json_result}\n")
        else:
            json_result = json.dumps({"error": "No results found"})
            print(f"No companies found with the specified name: {company_name}\n")

        end_time = time.time()
        first_execution_time = round((end_time - start_time) * 1000, 2)
        print(f"Response time (first run - Query 1): {first_execution_time} ms")
        first_execution_response_times[f"{percentage} - Query 1"] = first_execution_time

        average_time_subsequent, mean, margin_of_error = measure_query_performance(db, 1, percentage)
        print(f"Average time of 30 successive executions (Query 1): {average_time_subsequent} ms")
        print(f"Confidence interval (Query 1): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 1"] = (average_time_subsequent, mean, margin_of_error)

        # Query 2: Recupera i dettagli dell'azienda e dei suoi amministratori
        start_time = time.time()
        company_id, company, administrators = query2(db, percentage)
        if company:
            company_json = json.dumps(company, indent=4, default=json_serializer)
            # administrators_json = json.dumps(administrators, indent=4, default=json_serializer)
            print(f"Company details with ID {company_id} and administrators: \n{company_json}\n")
            # print(f"Dettagli degli amministratori dell'azienda con ID {company_id}: \n{administrators_json}\n")
        else:
            print(f"No companies found with ID {company_id}\n")

        end_time = time.time()
        first_execution_time = round((end_time - start_time) * 1000, 2)
        print(f"Response Time (First Run - Query 2): {first_execution_time} ms")
        first_execution_response_times[f"{percentage} - Query 2"] = first_execution_time

        average_time_subsequent, mean, margin_of_error = measure_query_performance(db, 2, percentage)
        print(f"Average time of 30 successive executions (Query 2): {average_time_subsequent} ms")
        print(f"Confidence Interval (Query 2): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 2"] = (average_time_subsequent, mean, margin_of_error)

        # Query 3: Recupera i dettagli dell'azienda, dei suoi amministratori e degli UBO con più del 25%
        start_time = time.time()
        company_id, company, administrators, ubos = query3(db, percentage)
        if company:
            company_json = json.dumps(company, indent=4, default=json_serializer)
            print(f"Company details with ID {company_id}, administrators and UBO: \n{company_json}\n")
        else:
            print(f"No companies found with ID {company_id}\n")
        end_time = time.time()
        first_execution_time = round((end_time - start_time) * 1000, 2)
        first_execution_response_times[f"{percentage} - Query 3"] = first_execution_time

        average_time_subsequent, mean, margin_of_error = measure_query_performance(db, 3, percentage)
        print(f"Average time of 30 successive executions (Query 3): {average_time_subsequent} ms")
        print(f"Confidence Interval (Query 3): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 3"] = (average_time_subsequent, mean, margin_of_error)

        # Query 4: Recupera i dettagli dell'azienda, dei suoi amministratori, UBO e la somma delle transazioni in un periodo
        start_time = time.time()
        company_id, company, administrators_details, ubos_details, total_amount = query4(db, percentage)
        if company:
            company_json = json.dumps(company, indent=4, default=json_serializer)
            print(f"Company details with ID {company_id}, administrators, UBO and transactions: \n{company_json}\n")
        else:
            print(f"No companies found with ID {company_id}\n")
        end_time = time.time()
        first_execution_time = round((end_time - start_time) * 1000, 2)
        first_execution_response_times[f"{percentage} - Query 4"] = first_execution_time

        average_time_subsequent, mean, margin_of_error = measure_query_performance(db, 4, percentage)
        print(f"Average time of 30 successive executions (Query 4): {average_time_subsequent} ms")
        print(f"Confidence Interval (Query 4): [{round(mean - margin_of_error, 2)}, {round(mean + margin_of_error, 2)}] ms\n")
        average_response_times[f"{percentage} - Query 4"] = (average_time_subsequent, mean, margin_of_error)

        print("-" * 70)  # Separatore tra le diverse percentuali

    # Scrivi i tempi di risposta della prima esecuzione su un file CSV
    with open('MongoDB/ResponseTimes/mongodb_times_of_response_first_execution.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Dataset', 'Query', 'Milliseconds'])
        for query, first_execution_time in first_execution_response_times.items():
            dataset, query = query.split(' - ')
            writer.writerow([dataset, query, first_execution_time])

    # Scrivi i tempi di risposta medi delle 30 esecuzioni su un altro file CSV
    with open('MongoDB/ResponseTimes/mongodb_response_times_average_30.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Dataset', 'Query', 'Milliseconds', 'Average', 'Confidence Interval (Min, Max)'])
        for query, (average_time_subsequent, mean_value, margin_of_error) in average_response_times.items():
            dataset, query_name = query.split(' - ')
            min_interval = round(mean_value - margin_of_error, 2)
            max_interval = round(mean_value + margin_of_error, 2)
            confidence_interval = f"({min_interval}, {max_interval})"  
            writer.writerow([dataset, query_name, average_time_subsequent, round(mean_value, 2), confidence_interval])
        print("Average response times were written in 'mongodb_times_of_response_first_execution.csv' and 'mongodb_response_times_average_30.csv'.")

# Esegui la funzione principale se il modulo è eseguito direttamente
if __name__ == "__main__":
    main()
