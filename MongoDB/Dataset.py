import random
import csv
from faker import Faker

# Crea un'istanza del generatore di dati falsi
fake = Faker()

# Definisci il numero di record da generare per ciascun tipo di entità
NUM_INDIVIDUALS = 1000
NUM_COMPANIES = 500
NUM_TRANSACTIONS = 2000
NUM_BANK_ACCOUNTS = 1000
NUM_ID_DOCUMENTS = 500
NUM_IP_ADDRESSES = 200
NUM_DEVICES = 200

# Genera i dati per gli individui
individuals = []
for individual_id in range(1, NUM_INDIVIDUALS + 1):
    name = fake.name()
    birthdate = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d')
    address = fake.address()
    phone_number = fake.phone_number()
    email = fake.email()
    tax_code = fake.ssn()
    individuals.append({
        'id': individual_id,
        'name': name,
        'birthdate': birthdate,
        'address': address,
        'phone_number': phone_number,
        'email': email,
        'tax_code': tax_code
    })

# Scrivi i dati degli individui in un file CSV
with open('Dataset/individuals.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'name', 'birthdate', 'address', 'phone_number', 'email', 'tax_code']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(individuals)

print("CSV file 'individuals.csv' successfully created.")

# Genera i dati per le aziende
companies = []
for company_id in range(1, NUM_COMPANIES + 1):
    name = fake.company()
    address = fake.address()
    phone_number = fake.phone_number()
    email = fake.company_email()
    vat_number = fake.ssn()
    directors = random.sample(range(1, NUM_INDIVIDUALS + 1), random.randint(1, 3))
    companies.append({
        'id': company_id,
        'name': name,
        'address': address,
        'phone_number': phone_number,
        'email': email,
        'vat_number': vat_number,
        'directors': directors
    })

# Scrivi i dati delle aziende in un file CSV
with open('Dataset/companies.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'name', 'address', 'phone_number', 'email', 'vat_number', 'directors']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for company in companies:
        company['directors'] = ','.join(map(str, company['directors']))
        writer.writerow(company)

print("CSV file 'companies.csv' successfully created.")

# Genera i dati per le transazioni finanziarie
transactions = []
for transaction_id in range(1, NUM_TRANSACTIONS + 1):
    amount = round(random.uniform(10.0, 10000.0), 2)
    date = fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
    origin_account = random.randint(1, NUM_BANK_ACCOUNTS)
    destination_account = random.randint(1, NUM_BANK_ACCOUNTS)
    while destination_account == origin_account:
        destination_account = random.randint(1, NUM_BANK_ACCOUNTS)
    transaction_type = random.choice(['purchase', 'payment', 'transfer'])
    transactions.append({
        'id': transaction_id,
        'amount': amount,
        'date': date,
        'origin_account': origin_account,
        'destination_account': destination_account,
        'type': transaction_type
    })

# Scrivi i dati delle transazioni in un file CSV
with open('Dataset/transactions.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'amount', 'date', 'origin_account', 'destination_account', 'type']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(transactions)

print("CSV file 'transactions.csv' successfully created.")

# Genera i dati per i conti bancari
bank_accounts = []
for account_id in range(1, NUM_BANK_ACCOUNTS + 1):
    account_number = fake.iban()
    holder_name = fake.name()
    account_type = random.choice(['current', 'savings'])
    bank = fake.company()
    bank_accounts.append({
        'id': account_id,
        'account_number': account_number,
        'holder_name': holder_name,
        'account_type': account_type,
        'bank': bank
    })

# Scrivi i dati dei conti bancari in un file CSV
with open('Dataset/bank_accounts.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'account_number', 'holder_name', 'account_type', 'bank']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(bank_accounts)

print("CSV file 'bank_accounts.csv' successfully created.")

# Genera i dati per i documenti di identità
id_documents = []
for doc_id in range(1, NUM_ID_DOCUMENTS + 1):
    doc_type = random.choice(['passport', 'ID card'])
    doc_number = fake.ssn()
    issue_date = fake.date_between(start_date='-10y', end_date='-1y').strftime('%Y-%m-%d')
    expiration_date = fake.date_between(start_date='today', end_date='+10y').strftime('%Y-%m-%d')
    issuer = fake.company()
    owner = random.randint(1, NUM_INDIVIDUALS)
    id_documents.append({
        'id': doc_id,
        'type': doc_type,
        'number': doc_number,
        'issue_date': issue_date,
        'expiration_date': expiration_date,
        'issuer': issuer,
        'owner': owner
    })

# Scrivi i dati dei documenti di identità in un file CSV
with open('Dataset/id_documents.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'type', 'number', 'issue_date', 'expiration_date', 'issuer', 'owner']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(id_documents)

print("CSV file 'id_documents.csv' successfully created.")

# Genera i dati per gli indirizzi IP
ip_addresses = []
for _ in range(1, NUM_IP_ADDRESSES + 1):
    ip_address = fake.ipv4()
    use_date = fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d')
    ip_addresses.append({
        'ip_address': ip_address,
        'use_date': use_date
    })

# Scrivi i dati degli indirizzi IP in un file CSV
with open('Dataset/ip_addresses.csv', 'w', newline='') as csvfile:
    fieldnames = ['ip_address', 'use_date']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(ip_addresses)

print("CSV file 'ip_addresses.csv' successfully created.")

# Genera i dati per i dispositivi
devices = []
for device_id in range(1, NUM_DEVICES + 1):
    device_type = random.choice(['computer', 'smartphone'])
    imsi = fake.msisdn() if device_type == 'smartphone' else ''
    devices.append({
        'id': device_id,
        'type': device_type,
        'imsi': imsi
    })

# Scrivi i dati dei dispositivi in un file CSV
with open('Dataset/devices.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'type', 'imsi']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(devices)

print("CSV file 'devices.csv' successfully created.")
