from neo4j import GraphDatabase

# Dati di connessione
uri = "bolt://localhost:7687"  # Modifica questo se il tuo Neo4j è in un'altra posizione
user = "neo4j"  # Modifica questo con il tuo nome utente
password = "12345678"  # Modifica questo con la tua password

# Elenco dei database da gestire
databases = ["dataset100", "dataset75", "dataset50", "dataset25"]

# Funzione per connettersi al database e eseguire una query
def drop_all_nodes_and_relationships(driver, database_name):
    with driver.session(database=database_name) as session:
        session.run("MATCH (n) DETACH DELETE n")
        print(f"All nodes and relationships have been dropped in database '{database_name}'.")

def main():
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        for db in databases:
            drop_all_nodes_and_relationships(driver, db)
    finally:
        driver.close()

if __name__ == "__main__":
    main()
