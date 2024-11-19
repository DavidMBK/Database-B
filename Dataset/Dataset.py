import random
import csv
from faker import Faker
import os

# Crea un'istanza del generatore di dati falsi
fake = Faker()

# Costanti di configurazione
NUM_PATIENTS = 400
NUM_DOCTORS = 15
NUM_PROCEDURES = 2
NUM_VISITS = 1200
SUBSET_DIR = 'Dataset/Subsets'  # Cartella dove vengono creati tutti i sottoinsiemi

# Crea la directory 'Subsets' se non esiste
os.makedirs(SUBSET_DIR, exist_ok=True)

# Funzione per generare e scrivere un dataset
def write_csv(file_path, data, fieldnames):
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"CSV file '{file_path}' created successfully.")

# Genera dati pazienti
patients = [
    {
        'id': patient_id,
        'name': fake.name(),
        'birthdate': fake.date_of_birth(minimum_age=65, maximum_age=90).strftime('%Y-%m-%d'),
        'address': fake.address(),
        'phone_number': fake.phone_number(),
        'email': fake.email()
    }
    for patient_id in range(1, NUM_PATIENTS + 1)
]

# Genera dati dottori
specializations = [
    'Cardiologist', 'Neurologist', 'Orthopedic Surgeon', 'Pediatrician', 'Dermatologist',
    'Gynecologist', 'Oncologist', 'Endocrinologist', 'Radiologist', 'General Surgeon'
]

doctors = [
    {
        'id': doctor_id,
        'name': fake.name(),
        'specialization': random.choice(specializations),
        'address': fake.address(),
        'phone_number': fake.phone_number(),
        'email': fake.email()
    }
    for doctor_id in range(1, NUM_DOCTORS + 1)
]

# Genera dati procedure
procedures = [
    {
        'id': procedure_id,
        'description': random.choice([
            'Complete Blood Count (CBC)', 'Basic Metabolic Panel', 'Lipid Panel', 'Liver Function Test',
            'Kidney Function Test', 'Thyroid Function Test', 'Chest X-Ray', 'Abdominal Ultrasound',
            'MRI of the Brain', 'CT Scan of the Abdomen', 'Electrocardiogram (ECG)', 'Stress Test'
        ]),
        'code': f'PR{procedure_id:04}'
    }
    for procedure_id in range(1, NUM_PROCEDURES + 1)
]

# Genera dati visite
def generate_visit_duration():
    return random.randint(10, 180)  # Durata in minuti

# Le visite devono essere associate a pazienti, medici e procedure esistenti
visits = [
    {
        'id': visit_id,
        'date': fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d'),
        'cost': round(random.uniform(100.0, 1000.0), 2),
        'patient_id': random.randint(1, NUM_PATIENTS),  # Paziente esistente
        'doctor_id': random.randint(1, NUM_DOCTORS),  # Medico esistente
        'procedure_id': random.randint(1, NUM_PROCEDURES),  # Procedura esistente
        'duration': generate_visit_duration()
    }
    for visit_id in range(1, NUM_VISITS + 1)
]

# Funzione per creare sottoinsiemi progressivi (100%, 75%, 50%, 25%)
def create_progressive_subset(data, percentages):
    subsets = []
    current_subset = data
    for percentage in percentages:
        subset_size = round(len(current_subset) * percentage / 100)
        current_subset = random.sample(current_subset, subset_size)
        subsets.append(current_subset)
    return subsets

# Percentuali da considerare: 100%, 75%, 50%, 25%
percentages = [100, 75, 50, 25]

# Generazione dei sottoinsiemi progressivi
datasets = [
    ('patients', patients, ['id', 'name', 'birthdate', 'address', 'phone_number', 'email']),
    ('doctors', doctors, ['id', 'name', 'specialization', 'address', 'phone_number', 'email']),
    ('procedures', procedures, ['id', 'description', 'code']),
    ('visits', visits, ['id', 'date', 'cost', 'patient_id', 'doctor_id', 'procedure_id', 'duration']),
]

# Creare tutti i sottoinsiemi progressivi (incluso il 100%)
for dataset_name, dataset_data, fieldnames in datasets:
    subsets = create_progressive_subset(dataset_data, percentages)
    
    # Salva ogni sottoinsieme nella cartella Subsets
    for i, subset in enumerate(subsets):
        subset_filename = f'{SUBSET_DIR}/{dataset_name}_{percentages[i]}percent.csv'
        write_csv(subset_filename, subset, fieldnames)
