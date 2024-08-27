import pandas as pd
from cassandra.cluster import Cluster
from uuid import uuid4
import random

def generate_uuid_map(dataframe, column_name):
    """Genera una mappa UUID per i valori unici in una colonna."""
    uuid_map = {}
    unique_values = dataframe[column_name].unique()
    for value in unique_values:
        uuid_map[value] = uuid4()
    return uuid_map

def insert_data(session, keyspace_name, patients, doctors, procedures, visits):
    session.set_keyspace(keyspace_name)
    
    try:
        # Genera UUID per i dati esistenti
        patient_id_map = generate_uuid_map(patients, 'id')
        doctor_id_map = generate_uuid_map(doctors, 'id')
        procedure_id_map = generate_uuid_map(procedures, 'id')

        # Inserire i dati nella tabella dei pazienti
        for _, row in patients.iterrows():
            session.execute("""
                INSERT INTO patients (id, name, birthdate, address, phone_number, email) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (patient_id_map[row['id']], row['name'], row['birthdate'], row['address'], row['phone_number'], row['email']))

        print("Patients data inserted.")

        # Inserire i dati nella tabella dei dottori
        for _, row in doctors.iterrows():
            session.execute("""
                INSERT INTO doctors (id, name, specialization, address, phone_number, email) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (doctor_id_map[row['id']], row['name'], row['specialization'], row['address'], row['phone_number'], row['email']))

        print("Doctors data inserted.")

        # Inserire i dati nella tabella delle procedure
        for _, row in procedures.iterrows():
            session.execute("""
                INSERT INTO procedures (id, description, code) 
                VALUES (%s, %s, %s)
            """, (procedure_id_map[row['id']], row['description'], row['code']))

        print("Procedures data inserted.")

        # Inserire i dati nella tabella delle visite
        for _, row in visits.iterrows():
            patient_id = patient_id_map.get(row['patient_id'])
            doctor_id = doctor_id_map.get(row['doctor_id'])
            procedure_id = procedure_id_map.get(row['procedure_id'])

            if patient_id and doctor_id and procedure_id:
                session.execute("""
                    INSERT INTO visits (id, date, cost, patient_id, doctor_id, procedure_id, duration) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (uuid4(), row['date'], row['cost'], patient_id, doctor_id, procedure_id, row['duration']))
            else:
                print(f"Visit record skipped due to missing references: {row}")

        print("Visits data inserted.")

        # Creare e popolare la tabella denormalizzata
        for _, row in visits.iterrows():
            patient_id = patient_id_map.get(row['patient_id'])
            doctor_id = doctor_id_map.get(row['doctor_id'])
            procedure_id = procedure_id_map.get(row['procedure_id'])

            if patient_id and doctor_id and procedure_id:
                # Recuperare i dettagli del paziente, del dottore e della procedura
                patient = patients[patients['id'] == row['patient_id']].iloc[0]
                doctor = doctors[doctors['id'] == row['doctor_id']].iloc[0]
                procedure = procedures[procedures['id'] == row['procedure_id']].iloc[0]

                # Inserire i dati nella tabella denormalizzata
                session.execute("""
                    INSERT INTO patient_visits (patient_id, visit_id, name, email, visit_date, doctor_name, procedure_name, visit_duration)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (patient_id, uuid4(), patient['name'], patient['email'], row['date'], doctor['name'], procedure['description'], row['duration']))

        print("Patient visits data inserted.")
    
    except Exception as e:
        print(f"Error during data insertion: {e}")

def create_keyspace_and_tables(session, keyspace_name):
    # Creare un keyspace se non esiste
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(keyspace_name)

    # Creare tabelle se non esistono
    tables = {
        'patients': """
            CREATE TABLE IF NOT EXISTS patients (
                id UUID PRIMARY KEY,
                name TEXT,
                birthdate TEXT,
                address TEXT,
                phone_number TEXT,
                email TEXT
            )
        """,
        'doctors': """
            CREATE TABLE IF NOT EXISTS doctors (
                id UUID PRIMARY KEY,
                name TEXT,
                specialization TEXT,
                address TEXT,
                phone_number TEXT,
                email TEXT
            )
        """,
        'procedures': """
            CREATE TABLE IF NOT EXISTS procedures (
                id UUID PRIMARY KEY,
                description TEXT,
                code TEXT
            )
        """,
        'visits': """
            CREATE TABLE IF NOT EXISTS visits (
                id UUID PRIMARY KEY,
                date TEXT,
                cost DECIMAL,
                patient_id UUID,
                doctor_id UUID,
                procedure_id UUID,
                duration INT
            )
        """,
        'patient_visits': """
            CREATE TABLE IF NOT EXISTS patient_visits (
                patient_id UUID,
                visit_id UUID,
                name TEXT,
                email TEXT,
                visit_date TIMESTAMP,
                doctor_name TEXT,
                procedure_name TEXT,
                visit_duration INT,
                PRIMARY KEY ((patient_id, visit_date), visit_id)
            )
        """
    }

    for table_name, create_table_query in tables.items():
        session.execute(create_table_query)

def main():
    # Connessione al cluster Cassandra
    cluster = Cluster(['127.0.0.1'], port=9042, connect_timeout=300)
    session = cluster.connect()

    try:
        # Percentuali dei dati
        percentages = [1.0, 0.75, 0.50, 0.25]
        
        for pct in percentages:
            keyspace_name = f"healthcare_{int(pct*100)}"
            print(f"Processing {keyspace_name}...")

            # Creare e impostare il keyspace e le tabelle
            create_keyspace_and_tables(session, keyspace_name)
    
            # Carica i dataset dai file CSV
            patients = pd.read_csv('./Dataset/patients.csv', encoding='ISO-8859-1')
            doctors = pd.read_csv('./Dataset/doctors.csv', encoding='ISO-8859-1')
            procedures = pd.read_csv('./Dataset/procedures.csv', encoding='ISO-8859-1')
            visits = pd.read_csv('./Dataset/visits.csv', encoding='ISO-8859-1')

            # Creare subset dei dati
            patients_subset = patients.sample(frac=pct, random_state=1)
            doctors_subset = doctors.sample(frac=pct, random_state=1)
            procedures_subset = procedures.sample(frac=pct, random_state=1)
            visits_subset = visits.sample(frac=pct, random_state=1)

            # Inserire i dati nei keyspace corrispondenti
            insert_data(session, keyspace_name, patients_subset, doctors_subset, procedures_subset, visits_subset)

            print(f"{keyspace_name} created and populated successfully.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        cluster.shutdown()

if __name__ == "__main__":
    main()
