from cassandra.cluster import Cluster

def fetch_table_data(session, keyspace_name, table_name):
    try:
        session.set_keyspace(keyspace_name)
        query = f"SELECT * FROM {table_name}"
        rows = session.execute(query)
        return rows
    except Exception as e:
        print(f"Error retrieving data from table '{table_name}' in keyspace '{keyspace_name}': {e}")
        return []

def main():
    cluster = Cluster(['127.0.0.1'], port=9042, connect_timeout=300)
    session = cluster.connect()

    keyspace_name = 'healthcare_25'  # Specifica il keyspace di interesse
    table_name = 'doctor_visits'  # Specifica la tabella di interesse

    try:
        print(f"Fetching data from table '{table_name}' in keyspace '{keyspace_name}'...")
        data = fetch_table_data(session, keyspace_name, table_name)
        
        count = 0
        if data:
            print(f"Data from table '{table_name}':")
            for row in data:
                print(row)
                count += 1  # Incrementa il contatore per ogni riga trovata
            print(f"\nTotal records found: {count}")
        else:
            print(f"No data found in table '{table_name}'.")
            print(f"\nTotal records found: {count}")  # Anche se non ci sono dati, mostra che il conteggio è 0

    except Exception as e:
        print(f"Error: {e}")

    finally:
        cluster.shutdown()

if __name__ == "__main__":
    main()
