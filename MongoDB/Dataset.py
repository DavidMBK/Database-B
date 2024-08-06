import random
import csv
from faker import Faker

# Crea un'istanza del generatore di dati falsi
fake = Faker()

# Definisci il numero di record da generare per ciascun tipo di entità
NUM_PATIENTS = 1000
NUM_DOCTORS = 100
NUM_PROCEDURES = 50
NUM_VISITS = 2000

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
doctors = []
for doctor_id in range(1, NUM_DOCTORS + 1):
    name = fake.name()
    specialization = fake.job()
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
procedures = []
for procedure_id in range(1, NUM_PROCEDURES + 1):
    description = fake.catch_phrase()
    code = fake.bothify(text='??##')
    procedures.append({
        'id': procedure_id,
        'description': description,
        'code': code
    })

# Scrivi i dati delle procedure in un file CSV
with open('Dataset/procedures.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'description', 'code']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(procedures)

print("CSV file 'procedures.csv' successfully created.")

# Genera i dati per le visite/claim
visits = []
for visit_id in range(1, NUM_VISITS + 1):
    patient_id = random.randint(1, NUM_PATIENTS)
    doctor_id = random.randint(1, NUM_DOCTORS)
    procedure_id = random.randint(1, NUM_PROCEDURES)
    date = fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
    cost = round(random.uniform(100.0, 1000.0), 2)
    visits.append({
        'id': visit_id,
        'patient_id': patient_id,
        'doctor_id': doctor_id,
        'procedure_id': procedure_id,
        'date': date,
        'cost': cost
    })

# Scrivi i dati delle visite in un file CSV
with open('Dataset/visits.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'patient_id', 'doctor_id', 'procedure_id', 'date', 'cost']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(visits)

print("CSV file 'visits.csv' successfully created.")
