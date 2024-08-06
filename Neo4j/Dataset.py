import pandas as pd
from py2neo import Graph, Node, Relationship
from faker import Faker
import random

# Inizializza l'oggetto Faker per generare dati fittizi
fake = Faker()


# Funzione per creare nodi e relazioni nel grafo Neo4j a partire dai dataset forniti.
def create_graph(graph, admins, shareholders, ubos, transactions, companies, kyc_aml_checks):
    
    # Crea nodi per gli amministratori
    admin_nodes = {}
    for _, row in admins.iterrows():
        admin_node = Node("Administrators", id=row['id'], name=row['name'], address=row['address'], birthdate=row['birthdate'], nationality=row['nationality'])
        graph.create(admin_node)
        admin_nodes[row['id']] = admin_node

    # Crea nodi per gli azionisti
    shareholder_nodes = {}
    for _, row in shareholders.iterrows():
        shareholder_node = Node("Shareholders", id=row['id'], name=row['name'], type=row['type'], ownership_percentage=row['ownership_percentage'], address=row['address'], date_of_birth=row['date_of_birth'], nationality=row['nationality'])
        graph.create(shareholder_node)
        shareholder_nodes[row['id']] = shareholder_node

    # Crea nodi per gli UBO (Ultimate Beneficial Owners)
    ubo_nodes = {}
    for _, row in ubos.iterrows():
        ubo_node = Node("Ubo", id=row['id'], name=row['name'], address=row['address'], birthdate=row['birthdate'], nationality=row['nationality'], ownership_percentage=row['ownership_percentage'], type=row['type'])
        graph.create(ubo_node)
        ubo_nodes[row['id']] = ubo_node

    # Crea nodi per le transazioni
    transaction_nodes = {}
    for _, row in transactions.iterrows():
        transaction_node = Node("Transactions", id=row['id'], type=row['type'], amount=row['amount'], date=row['date'], currency=row['currency'])
        graph.create(transaction_node)
        transaction_nodes[row['id']] = transaction_node

    # Crea nodi per le aziende e le relazioni con altri nodi
    for _, row in companies.iterrows():
        company_node = Node("Companies", id=row['id'], name=row['name'], address=row['address'], legal_form=row['legal_form'], registration_details=row['registration_details'], financial_data=row['financial_data'])
        graph.create(company_node)

        # Crea relazioni tra aziende e amministratori
        for admin_id in eval(row['administrators']):
            if admin_id in admin_nodes:
                rel = Relationship(company_node, "COMPANY_HAS_ADMINISTRATOR", admin_nodes[admin_id], role="Administrator", start_date=fake.date_between(start_date='-5y', end_date='-1y'), end_date=None)
                graph.create(rel)

        # Crea relazioni tra aziende e azionisti
        for shareholder_id in eval(row['shareholders']):
            if shareholder_id in shareholder_nodes:
                rel = Relationship(company_node, "COMPANY_HAS_SHAREHOLDER", shareholder_nodes[shareholder_id], ownership_percentage=random.uniform(0.1, 100), purchase_date=fake.date_between(start_date='-10y', end_date='-1y'))
                graph.create(rel)

        # Crea relazioni tra aziende e UBO
        for ubo_id in eval(row['ubo']):
            if ubo_id in ubo_nodes:
                rel = Relationship(company_node, "COMPANY_HAS_UBO", ubo_nodes[ubo_id], ownership_percentage=random.uniform(0.1, 100), purchase_date=fake.date_between(start_date='-10y', end_date='-1y'))
                graph.create(rel)

        # Crea relazioni tra aziende e transazioni
        for transaction_id in eval(row['transactions']):
            if transaction_id in transaction_nodes:
                rel = Relationship(company_node, "COMPANY_HAS_TRANSACTION", transaction_nodes[transaction_id], transaction_type=random.choice(['Purchase', 'Sale', 'Payment', 'Refund']), amount=random.uniform(10.0, 10000.0), date=fake.date_between(start_date='-5y', end_date='today'), currency=random.choice(['EUR', 'USD', 'GBP', 'JPY', 'AUD']))
                graph.create(rel)

    # Crea relazioni per i controlli KYC/AML
    for _, row in kyc_aml_checks.iterrows():
        if row['ubo_id'] in ubo_nodes:
            rel = Relationship(ubo_nodes[row['ubo_id']], "UBO_HAS_CHECKS", Node("KYC_AML_Check", id=row['id'], check_type=row['type'], result=row['result'], date=row['date'], notes=row['notes']), check_type=row['type'], result=row['result'], date=row['date'], notes=row['notes'])
            graph.create(rel)

# Carica i dataset dai file CSV
admins = pd.read_csv('Dataset/administrators.csv', encoding='ISO-8859-1')
shareholders = pd.read_csv('Dataset/shareholders.csv', encoding='ISO-8859-1')
ubos = pd.read_csv('Dataset/ubo.csv', encoding='ISO-8859-1')
transactions = pd.read_csv('Dataset/transactions.csv', encoding='ISO-8859-1')
companies = pd.read_csv('Dataset/companies.csv', encoding='ISO-8859-1')
kyc_aml_checks = pd.read_csv('Dataset/kyc_aml_checks.csv', encoding='ISO-8859-1')

# Connessione ai database Neo4j
graph100 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset100")
graph75 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset75")
graph50 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset50")
graph25 = Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset25")

# Crea i grafi per i diversi dataset
create_graph(graph100, admins, shareholders, ubos, transactions, companies, kyc_aml_checks)
create_graph(graph75, admins.sample(frac=0.75), shareholders.sample(frac=0.75), ubos.sample(frac=0.75), transactions.sample(frac=0.75), companies.sample(frac=0.75), kyc_aml_checks.sample(frac=0.75))
create_graph(graph50, admins.sample(frac=0.50), shareholders.sample(frac=0.50), ubos.sample(frac=0.50), transactions.sample(frac=0.50), companies.sample(frac=0.50), kyc_aml_checks.sample(frac=0.50))
create_graph(graph25, admins.sample(frac=0.25), shareholders.sample(frac=0.25), ubos.sample(frac=0.25), transactions.sample(frac=0.25), companies.sample(frac=0.25), kyc_aml_checks.sample(frac=0.25))

# Stampa un messaggio di conferma
print("Data successfully loaded into Neo4j.")
