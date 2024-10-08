import pandas as pd
from cassandra.cluster import Cluster
from uuid import uuid4

def generate_uuid_map(dataframe, column_name):
    """Genera una mappa UUID per i valori unici in una colonna."""
    uuid_map = {}
    unique_values = dataframe[column_name].unique()
    for value in unique_values:
        uuid_map[value] = uuid4()
    return uuid_map

def create_keyspace_and_tables(session, keyspace_name):
    """Crea il keyspace e tutte le tabelle necessarie se non esistono."""
    # Creare un keyspace se non esiste
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(keyspace_name)

    # Definizione delle tabelle
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
                date DATE,
                cost DECIMAL,
                patient_id UUID,
                doctor_id UUID,
                procedure_id UUID,
                duration INT
            )
        """,
        'patient_visit_counts': """
            CREATE TABLE IF NOT EXISTS patient_visit_counts (
                patient_id UUID,
                visit_date DATE,
                visit_count COUNTER,
                PRIMARY KEY ((patient_id), visit_date)
            )
        """,
        'doctor_visits': """
            CREATE TABLE IF NOT EXISTS doctor_visits (
                doctor_id UUID,
                specialization TEXT,
                visit_date DATE,
                visit_count COUNTER,
                PRIMARY KEY ((doctor_id, specialization), visit_date)
            )
        """,
        'procedure_visit_stats': """
            CREATE TABLE IF NOT EXISTS procedure_visit_stats (
                procedure_id UUID,
                doctor_specialization TEXT,
                visit_date DATE,
                procedure_count COUNTER,
                PRIMARY KEY ((procedure_id, doctor_specialization), visit_date)
            )
        """,
        'doctor_patient_counts': """
            CREATE TABLE IF NOT EXISTS doctor_patient_counts (
                doctor_id UUID,
                visit_date DATE,
                total_patients COUNTER,
                PRIMARY KEY ((doctor_id), visit_date)
            )
        """
    }

    # Creare tutte le tabelle
    for table_name, create_table_query in tables.items():
        session.execute(create_table_query)
        print(f"Table '{table_name}' created or already exists.")

def sample_connected_data(visits, patients, doctors, procedures, percentage, random_state=1):
    """Seleziona un sottoinsieme dei dati mantenendo la coerenza delle relazioni tra pazienti, dottori e procedure."""
    
    # Prendere un sottoinsieme delle visite utilizzando un seed fisso per garantire coerenza tra le selezioni
    visits_subset = visits.sample(frac=percentage, random_state=random_state)
    
    # Estrarre gli ID collegati dai sottoinsiemi di visite
    patient_ids = visits_subset['patient_id'].unique()
    doctor_ids = visits_subset['doctor_id'].unique()
    procedure_ids = visits_subset['procedure_id'].unique()
    
    # Filtrare pazienti, dottori e procedure in base agli ID trovati nelle visite selezionate
    patients_subset = patients[patients['id'].isin(patient_ids)]
    doctors_subset = doctors[doctors['id'].isin(doctor_ids)]
    procedures_subset = procedures[procedures['id'].isin(procedure_ids)]
    
    return patients_subset, doctors_subset, procedures_subset, visits_subset

def insert_data(session, keyspace_name, patients, doctors, procedures, visits):
    """Inserisce i dati nelle tabelle e aggiorna i contatori."""
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
        
        # Inserire i dati nella tabella delle visite e aggiornare i contatori
        for _, row in visits.iterrows():
            patient_id = patient_id_map.get(row['patient_id'])
            doctor_id = doctor_id_map.get(row['doctor_id'])
            procedure_id = procedure_id_map.get(row['procedure_id'])
            
            if patient_id and doctor_id and procedure_id:
                # Inserimento nella tabella 'visits'
                session.execute("""
                    INSERT INTO visits (id, date, cost, patient_id, doctor_id, procedure_id, duration) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (uuid4(), row['date'], row['cost'], patient_id, doctor_id, procedure_id, row['duration']))
                
                # Aggiornamento dei contatori
                visit_date = pd.to_datetime(row['date']).date()  # Convertire 'row['date']' in formato DATE
                
                # Recuperare la specializzazione del dottore
                doctor_specialization = doctors.loc[doctors['id'] == row['doctor_id'], 'specialization'].values[0]
                
                # Aggiornamento di patient_visit_counts
                session.execute("""
                    UPDATE patient_visit_counts 
                    SET visit_count = visit_count + 1 
                    WHERE patient_id = %s 
                    AND visit_date = %s
                """, (patient_id, visit_date))
                
                # Aggiornamento di doctor_visits
                session.execute("""
                    UPDATE doctor_visits 
                    SET visit_count = visit_count + 1
                    WHERE doctor_id = %s 
                    AND specialization = %s
                    AND visit_date = %s
                """, (doctor_id, doctor_specialization, visit_date))
                
                # Aggiornamento di procedure_visit_stats
                session.execute("""
                    UPDATE procedure_visit_stats 
                    SET procedure_count = procedure_count + 1
                    WHERE procedure_id = %s 
                    AND doctor_specialization = %s
                    AND visit_date = %s
                """, (procedure_id, doctor_specialization, visit_date))
                
                # Aggiornamento di doctor_patient_counts
                session.execute("""
                    UPDATE doctor_patient_counts 
                    SET total_patients = total_patients + 1
                    WHERE doctor_id = %s 
                    AND visit_date = %s
                """, (doctor_id, visit_date))
            else:
                print(f"Visit record skipped due to missing references: {row}")
        print("Visits data inserted and counters updated.")
        
    except Exception as e:
        print(f"Error during data insertion: {e}")

def main():
    """Funzione principale per connettersi al cluster, creare keyspace e tabelle, e inserire i dati."""
    # Connessione al cluster Cassandra
    cluster = Cluster(['127.0.0.1'], port=9042, connect_timeout=300)
    session = cluster.connect()
    
    try:
        # Percentuali dei dati
        percentages = [1.0, 0.75, 0.50, 0.25]
        
        # Carica i dataset dai file CSV
        patients = pd.read_csv('./Dataset/patients.csv', encoding='ISO-8859-1')
        doctors = pd.read_csv('./Dataset/doctors.csv', encoding='ISO-8859-1')
        procedures = pd.read_csv('./Dataset/procedures.csv', encoding='ISO-8859-1')
        visits = pd.read_csv('./Dataset/visits.csv', encoding='ISO-8859-1')
        
        # Inizialmente usa il 100% dei dati
        patients_subset = patients
        doctors_subset = doctors
        procedures_subset = procedures
        visits_subset = visits
        
        for pct in percentages:
            keyspace_name = f"healthcare_{int(pct*100)}"
            print(f"Processing keyspace '{keyspace_name}'...")
            
            # Creare e impostare il keyspace e le tabelle
            create_keyspace_and_tables(session, keyspace_name)
            
            # Creare subset dei dati mantenendo la coerenza
            patients_subset, doctors_subset, procedures_subset, visits_subset = sample_connected_data(
                visits, patients, doctors, procedures, pct, random_state=1  # Usa random_state per consistenza
            )
            
            # Inserire i dati nei keyspace corrispondenti
            insert_data(session, keyspace_name, patients_subset, doctors_subset, procedures_subset, visits_subset)
            
            print(f"Keyspace '{keyspace_name}' created and populated successfully.\n")
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    main()
