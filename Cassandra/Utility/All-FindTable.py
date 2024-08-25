from cassandra.cluster import Cluster

def list_keyspaces(session):
    try:
        query = "SELECT keyspace_name FROM system_schema.keyspaces"
        rows = session.execute(query)
        return [row.keyspace_name for row in rows if row.keyspace_name.startswith('healthcare_')]
    except Exception as e:
        print(f"Error retrieving keyspaces: {e}")
        return []

def list_specific_tables_in_keyspace(session, keyspace_name):
    try:
        session.set_keyspace(keyspace_name)
        query = "SELECT table_name FROM system_schema.tables"
        rows = session.execute(query)
        # Filter tables to include only specific ones
        specific_tables = {'doctors', 'patients', 'procedures', 'visits'}
        return sorted(set(row.table_name for row in rows if row.table_name in specific_tables))
    except Exception as e:
        print(f"Error retrieving tables for keyspace '{keyspace_name}': {e}")
        return []

def main():
    cluster = Cluster(['127.0.0.1'], port=9042, connect_timeout=300)
    session = cluster.connect()

    try:
        # Recupera tutti i keyspace con il prefisso 'healthcare_'
        keyspaces = list_keyspaces(session)
        
        if not keyspaces:
            print("No keyspaces found.")
            return
        
        for keyspace in keyspaces:
            print(f"\nKeyspace: {keyspace}")
            # Recupera e visualizza solo le tabelle specifiche per ogni keyspace
            tables = list_specific_tables_in_keyspace(session, keyspace)
            if tables:
                print("Tables:")
                for table in tables:
                    print(f" - {table}")
            else:
                print("No specific tables found.")
    
    except Exception as e:
        print(f"Error: {e}")

    finally:
        cluster.shutdown()

if __name__ == "__main__":
    main()
