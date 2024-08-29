import random
import csv
from faker import Faker
import os

# Crea un'istanza del generatore di dati falsi
fake = Faker()

# Definisci il numero di record da generare per ciascun tipo di entità
NUM_PATIENTS = 50000
NUM_DOCTORS = 1000
NUM_PROCEDURES = 25000
NUM_VISITS = 150000

# Crea la directory 'Dataset' se non esiste
if not os.path.exists('Dataset'):
    os.makedirs('Dataset')

# Genera i dati per i pazienti
patients = []
for patient_id in range(1, NUM_PATIENTS + 1):
    name = fake.name()
    birthdate = fake.date_of_birth(minimum_age=65, maximum_age=90).strftime('%Y-%m-%d')
    address = fake.address()
    phone_number = fake.phone_number()
    email = fake.email()
    patients.append({
        'id': patient_id,
        'name': name,
        'birthdate': birthdate,
        'address': address,
        'phone_number': phone_number,
        'email': email
    })

# Scrivi i dati dei pazienti in un file CSV
with open('Dataset/patients.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'name', 'birthdate', 'address', 'phone_number', 'email']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(patients)

print("CSV file 'patients.csv' successfully created.")

# Genera i dati per i dottori
specializations = [
    'Cardiologist', 'Neurologist', 'Orthopedic Surgeon', 'Pediatrician', 'Dermatologist',
    'Gynecologist', 'Oncologist', 'Endocrinologist', 'Radiologist', 'General Surgeon'
]

doctors = []
for doctor_id in range(1, NUM_DOCTORS + 1):
    name = fake.name()
    specialization = random.choice(specializations)
    address = fake.address()
    phone_number = fake.phone_number()
    email = fake.email()
    doctors.append({
        'id': doctor_id,
        'name': name,
        'specialization': specialization,
        'address': address,
        'phone_number': phone_number,
        'email': email
    })

# Scrivi i dati dei dottori in un file CSV
with open('Dataset/doctors.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'name', 'specialization', 'address', 'phone_number', 'email']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(doctors)

print("CSV file 'doctors.csv' successfully created.")

# Genera i dati per le procedure mediche
procedures = [
    'Complete Blood Count (CBC)', 'Basic Metabolic Panel', 'Lipid Panel', 'Liver Function Test', 
    'Kidney Function Test', 'Thyroid Function Test', 'Chest X-Ray', 'Abdominal Ultrasound', 
    'MRI of the Brain', 'CT Scan of the Abdomen', 'Electrocardiogram (ECG)', 'Stress Test', 
    'Pulmonary Function Test', 'Colonoscopy', 'Mammogram', 'Pap Smear', 'Prostate-Specific Antigen (PSA) Test', 
    'Bone Density Test', 'Skin Biopsy', 'Endoscopy', 'Laparoscopy', 'Hearing Test', 
    'Eye Exam', 'Diabetes Screening', 'HIV Test', 'Hepatitis B Test', 'Hepatitis C Test', 
    'Allergy Testing', 'Genetic Testing', 'Drug Screening', 'Vaccination', 'Travel Vaccination', 
    'Preoperative Assessment', 'Postoperative Follow-Up', 'Nutritional Counseling', 'Psychiatric Evaluation', 
    'Physical Therapy', 'Speech Therapy', 'Occupational Therapy', 'Wound Care', 'Pain Management'
]

def generate_procedure_code(procedure_id):
    return f'PR{procedure_id:04}'

procedures_list = []
for procedure_id in range(1, NUM_PROCEDURES + 1):
    description = random.choice(procedures)
    code = generate_procedure_code(procedure_id)
    procedures_list.append({
        'id': procedure_id,
        'description': description,
        'code': code
    })

# Scrivi i dati delle procedure in un file CSV
with open('Dataset/procedures.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'description', 'code']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(procedures_list)

print("CSV file 'procedures.csv' successfully created.")

# Genera i dati per le visite/claim
def generate_visit_duration():
    """Genera una durata casuale tra 10 minuti e 3 ore."""
    return random.randint(10, 180)  # Durata in minuti

visits = []
for visit_id in range(1, NUM_VISITS + 1):
    patient_id = random.randint(1, NUM_PATIENTS)
    doctor_id = random.randint(1, NUM_DOCTORS)
    procedure_id = random.randint(1, NUM_PROCEDURES)
    date = fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
    cost = round(random.uniform(100.0, 1000.0), 2)
    duration = generate_visit_duration()
    visits.append({
        'id': visit_id,
        'date': date,
        'cost': cost,
        'patient_id': patient_id,
        'doctor_id': doctor_id,
        'procedure_id': procedure_id,
        'duration': duration
    })

# Scrivi i dati delle visite in un file CSV
with open('Dataset/visits.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'date', 'cost', 'patient_id', 'doctor_id', 'procedure_id', 'duration']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(visits)

print("CSV file 'visits.csv' successfully created.")
