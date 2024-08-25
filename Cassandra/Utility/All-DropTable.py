from cassandra.cluster import Cluster

def describe_keyspace(session, keyspace_name):
    try:
        query = f"SELECT * FROM system_schema.keyspaces WHERE keyspace_name='{keyspace_name}'"
        rows = session.execute(query)
        if rows:
            for row in rows:
                print(f"Keyspace Name: {row.keyspace_name}")
                print(f"Replication: {row.replication}")
                print(f"Durable Writes: {row.durable_writes}")
        else:
            print(f"Keyspace '{keyspace_name}' does not exist.")
    except Exception as e:
        print(f"Error during keyspace description: {e}")

def describe_tables(session, keyspace_name):
    try:
        query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{keyspace_name}'"
        rows = session.execute(query)
        tables = [row.table_name for row in rows]
        if tables:
            print(f"Tables in keyspace '{keyspace_name}':")
            for table in tables:
                print(f"- {table}")
        else:
            print(f"No tables found in keyspace '{keyspace_name}'.")
        return tables
    except Exception as e:
        print(f"Error during tables description: {e}")
        return []

def drop_tables(session, keyspace_name):
    tables = describe_tables(session, keyspace_name)
    for table in tables:
        try:
            query = f"DROP TABLE IF EXISTS {keyspace_name}.{table}"
            session.execute(query)
            print(f"Table '{table}' has been dropped.")
        except Exception as e:
            print(f"Error dropping table '{table}': {e}")

def drop_keyspace(session, keyspace_name):
    try:
        query = f"DROP KEYSPACE IF EXISTS {keyspace_name}"
        session.execute(query)
        print(f"Keyspace '{keyspace_name}' has been dropped.")
    except Exception as e:
        print(f"Error dropping keyspace '{keyspace_name}': {e}")

def main():
    # Connessione al cluster Cassandra
    cluster = Cluster(['127.0.0.1'], port=9042, connect_timeout=300)
    session = cluster.connect()

    try:
        # Lista dei keyspace con varie percentuali
        keyspaces = ['healthcare_100', 'healthcare_75', 'healthcare_50', 'healthcare_25']

        for keyspace in keyspaces:
            print(f"\nProcessing keyspace: {keyspace}")
            describe_keyspace(session, keyspace)
            drop_tables(session, keyspace)
            drop_keyspace(session, keyspace)

    finally:
        # Chiudi la connessione
        cluster.shutdown()

if __name__ == "__main__":
    main()
