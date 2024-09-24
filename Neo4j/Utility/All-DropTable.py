from neo4j import GraphDatabase

# Dati di connessione
uri = "bolt://localhost:7687"  # Modifica questo se il tuo Neo4j è in un'altra posizione
user = "neo4j"  # Modifica questo con il tuo nome utente
password = "12345678"  # Modifica questo con la tua password

# Elenco dei database da gestire
databases = ["dataset100", "dataset75", "dataset50", "dataset25"]

# Funzione per connettersi al database ed eseguire la cancellazione in batch
def drop_nodes_and_relationships_in_batches(driver, database_name, batch_size=5000):
    with driver.session(database=database_name) as session:
        while True:
            # Controlla il numero totale di nodi
            result = session.run("MATCH (n) RETURN count(n) AS count")
            total_nodes = result.single()["count"]
            
            if total_nodes == 0:
                break

            print(f"Found {total_nodes} nodes in database '{database_name}'. Deleting in batches of {batch_size}...")

            # Esegui la cancellazione dei nodi e delle relazioni in batch
            session.write_transaction(lambda tx: tx.run(f"""
                MATCH (n)
                WITH n LIMIT {batch_size}
                DETACH DELETE n
            """))

            # Log progress
            print(f"Deleted {batch_size} nodes, checking remaining...")

        print(f"All nodes and relationships have been dropped in database '{database_name}'.")

def main():
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        for db in databases:
            drop_nodes_and_relationships_in_batches(driver, db)
    finally:
        driver.close()

if __name__ == "__main__":
    main()
