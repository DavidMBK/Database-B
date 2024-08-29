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

    # Crea nodi per le visite/claim e relazioni con altri nodi
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

# Carica i dataset dai file CSV
patients = pd.read_csv('Dataset/patients.csv', encoding='ISO-8859-1')
doctors = pd.read_csv('Dataset/doctors.csv', encoding='ISO-8859-1')
procedures = pd.read_csv('Dataset/procedures.csv', encoding='ISO-8859-1')
visits = pd.read_csv('Dataset/visits.csv', encoding='ISO-8859-1')

# Connessione ai diversi database Neo4j
graph100 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset100")
graph75 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset75")
graph50 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset50")
graph25 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset25")

# Crea i grafi per i diversi dataset
create_graph(graph100, patients, doctors, procedures, visits, "dataset100")
create_graph(graph75, patients.sample(frac=0.75), doctors.sample(frac=0.75), procedures.sample(frac=0.75), visits.sample(frac=0.75), "dataset75")
create_graph(graph50, patients.sample(frac=0.50), doctors.sample(frac=0.50), procedures.sample(frac=0.50), visits.sample(frac=0.50), "dataset50")
create_graph(graph25, patients.sample(frac=0.25), doctors.sample(frac=0.25), procedures.sample(frac=0.25), visits.sample(frac=0.25), "dataset25")

# Stampa un messaggio di conferma
print("Data successfully loaded into all Neo4j databases.")
