import os
import pandas as pd
from py2neo import Graph, Node, Relationship
from tqdm import tqdm

# Funzione per creare nodi e relazioni nel grafo Neo4j a partire dai dataset forniti
def create_graph(graph, patients, doctors, procedures, visits, dataset_name):
    print(f"Starting to load data into {dataset_name}...")

    # Crea nodi per i pazienti
    patient_nodes = {}
    for _, row in tqdm(patients.iterrows(), total=len(patients), desc=f"Loading Patients into {dataset_name}"):
        patient_node = Node("Patient", id=row['id'], name=row['name'], birthdate=row['birthdate'], address=row['address'], phone_number=row['phone_number'], email=row['email'])
        graph.create(patient_node)
        patient_nodes[row['id']] = patient_node

    # Crea nodi per i dottori
    doctor_nodes = {}
    for _, row in tqdm(doctors.iterrows(), total=len(doctors), desc=f"Loading Doctors into {dataset_name}"):
        doctor_node = Node("Doctor", id=row['id'], name=row['name'], specialization=row['specialization'], address=row['address'], phone_number=row['phone_number'], email=row['email'])
        graph.create(doctor_node)
        doctor_nodes[row['id']] = doctor_node

    # Crea nodi per le procedure mediche
    procedure_nodes = {}
    for _, row in tqdm(procedures.iterrows(), total=len(procedures), desc=f"Loading Procedures into {dataset_name}"):
        procedure_node = Node("Procedure", id=row['id'], description=row['description'], code=row['code'])
        graph.create(procedure_node)
        procedure_nodes[row['id']] = procedure_node

    # Crea nodi per le visite e relazioni
    for _, row in tqdm(visits.iterrows(), total=len(visits), desc=f"Loading Visits and Relationships into {dataset_name}"):
        visit_node = Node("Visit", id=row['id'], date=row['date'], cost=row['cost'], duration=row['duration'])
        graph.create(visit_node)

        # Relazione tra visita e paziente
        if row['patient_id'] in patient_nodes:
            rel_patient = Relationship(visit_node, "VISIT_BY", patient_nodes[row['patient_id']])
            graph.create(rel_patient)

        # Relazione tra visita e dottore
        if row['doctor_id'] in doctor_nodes:
            rel_doctor = Relationship(visit_node, "VISIT_TO", doctor_nodes[row['doctor_id']])
            graph.create(rel_doctor)

        # Relazione tra visita e procedura
        if row['procedure_id'] in procedure_nodes:
            rel_procedure = Relationship(visit_node, "INCLUDES_PROCEDURE", procedure_nodes[row['procedure_id']])
            graph.create(rel_procedure)

    print(f"Finished loading data into {dataset_name}.")

# Funzione per caricare i dati dal file CSV
def load_data_from_csv(dataset_path):
    return pd.read_csv(dataset_path, encoding='ISO-8859-1')

# Percorso della cartella Subsets
SUBSET_DIR = 'Dataset/Subsets'

# Connessione ai diversi database Neo4j
graph100 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset100")
graph75 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset75")
graph50 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset50")
graph25 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset25")

# Funzione per caricare i dati per ciascun sottoinsieme e categoria
def load_and_create_graph_for_subset(subset_percentage, graph):
    # Definisci i nomi dei file CSV basati sulla percentuale
    patients_file = f'{SUBSET_DIR}/patients_{subset_percentage}percent.csv'
    doctors_file = f'{SUBSET_DIR}/doctors_{subset_percentage}percent.csv'
    procedures_file = f'{SUBSET_DIR}/procedures_{subset_percentage}percent.csv'
    visits_file = f'{SUBSET_DIR}/visits_{subset_percentage}percent.csv'

    # Carica i dati dai file CSV
    patients = load_data_from_csv(patients_file)
    doctors = load_data_from_csv(doctors_file)
    procedures = load_data_from_csv(procedures_file)
    visits = load_data_from_csv(visits_file)

    # Crea i nodi e le relazioni nel grafo
    create_graph(graph, patients, doctors, procedures, visits, f"dataset{subset_percentage}")

# Carica i dati e crea i grafi per i vari sottoinsiemi
load_and_create_graph_for_subset(100, graph100)
load_and_create_graph_for_subset(75, graph75)
load_and_create_graph_for_subset(50, graph50)
load_and_create_graph_for_subset(25, graph25)

# Stampa un messaggio di conferma
print("Data successfully loaded into all Neo4j databases.")
