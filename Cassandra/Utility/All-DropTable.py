from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

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
            statement = SimpleStatement(query, consistency_level=ConsistencyLevel.ONE, fetch_size=10)  # Adjust fetch_size if necessary
            session.execute(statement, timeout=600)  # Set timeout in seconds
            print(f"Table '{table}' has been dropped.")
        except Exception as e:
            print(f"Error dropping table '{table}': {e}. Query: {query}")

def drop_keyspace(session, keyspace_name):
    try:
        query = f"DROP KEYSPACE IF EXISTS {keyspace_name}"
        statement = SimpleStatement(query, consistency_level=ConsistencyLevel.ONE)
        session.execute(statement, timeout=600)  # Set timeout in seconds
        print(f"Keyspace '{keyspace_name}' has been dropped.")
    except Exception as e:
        print(f"Error dropping keyspace '{keyspace_name}': {e}")

def main():
    # Connect to the Cassandra cluster
    cluster = Cluster(['127.0.0.1'], port=9042, connect_timeout=300)  # Removed request_timeout
    session = cluster.connect()

    try:
        # Retrieve all keyspaces
        query = "SELECT keyspace_name FROM system_schema.keyspaces"
        rows = session.execute(query)
        keyspaces = [row.keyspace_name for row in rows]

        for keyspace in keyspaces:
            # Skip the 'system' keyspaces that are internal to Cassandra
            if keyspace.startswith('system'):
                continue
            
            print(f"\nProcessing keyspace: {keyspace}")
            describe_keyspace(session, keyspace)
            drop_tables(session, keyspace)
            drop_keyspace(session, keyspace)

    finally:
        # Close the connection
        cluster.shutdown()

if __name__ == "__main__":
    main()
