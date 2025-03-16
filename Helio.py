from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import csv
import os 

# Macros
BATCH_SIZE = 10000

# Mappings 
metaedge_to_relationship = {
    'CrC': 'Resembles',
    'CtD': 'Treats',
    'CpD': 'Palliates',
    'CbG': 'Binds',
    'CuG': 'Upregulates',
    'CdG': 'Downregulates',
    'DrD': 'Resembles',
    'DuG': 'Upregulates',
    'DdG': 'Downregulates',
    'DaG': 'Associates',
    'DlA': 'Localizes',
    'AuG': 'Upregulates',
    'AdG': 'Downregulates',
    'AeG': 'Expresses',
    'Gr>G': 'Regulates',
    'GcG': 'Covariates',
    'GiG': 'Interacts'
}

# Load the .env file 
load_dotenv() 
# Now you can access the environment variables

mongo_user = os.getenv("MONGOUSER")
mongo_password = os.getenv("MONGOPASSWORD")
uri = "mongodb+srv://{}:{}@mongocluster.xwtr8.mongodb.net/?retryWrites=true&w=majority&appName=MongoCluster".format(mongo_user, mongo_password)

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
db_name = "helio_net"
database = client[db_name]

node_file = "nodes.tsv"
edge_file = "edges.tsv"

def insert_nodes():
    # node_file = "sample_nodes.tsv"
    collection_name = "nodes"
    collection = database[collection_name]
    with open(node_file, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        #skip the header row 
        next(reader, None)

        batch = []
        for row in reader:
            id, name, kind = row[0], row[1], row[2]
            document_to_insert = {
                'id': id, 
                'name': name,
                'kind': kind
            }
            batch.append(document_to_insert)
            if len(batch) == BATCH_SIZE:
                collection.insert_many(batch)
                batch = [] 
        
        # Insert remaining documents
        for d in batch:
            collection.insert_one(d)
    print("node insertion complete.")
def insert_edges():
    # edge_file = "sample_edges.tsv"
    collection_name = "edges"
    collection = database[collection_name]
    with open(edge_file, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        #skip the header row 
        next(reader, None) 

        batch = [] 
        for row in reader:
            source, metaedge, target = row[0], row[1], row[2]
            document_to_insert = {
                'source': source, 
                'metaedge': metaedge, 
                'target': target
            }
            batch.append(document_to_insert)
            if (len(batch) == BATCH_SIZE):
                collection.insert_many(batch)
                batch = [] 
        # Insert remaining documents
        for d in batch:
            collection.insert_one(d)
    print("edge insertion complete.")
# Deletes all documents in the collection
def del_collection(collection_name):
    collection = database[collection_name]
    collection.delete_many({})

# Query 1
def query_one():
    collection_name = 'edges'
    collection = database[collection_name]

    disease_id = "Disease::DOID:0050425"
    pipeline = [
        {
            "$match": {
                "$or": [
                    {"$and": [{"metaedge": {"$in": ["CtD", "CpD"]}}, {"target": disease_id}]},
                    {"$and": [{"metaedge": "DaG"}, {"source": disease_id}]},
                    {"$and": [{"metaedge": "DlA"}, {"source": disease_id}]}]
            }
        },
        {
            "$group": {
                "_id": disease_id,
                "compound_names": {"$addToSet": "$source"},
                "disease_names": {"$addToSet": "$target"},
                "gene_names": {"$addToSet": "$target"},   # Fix: Get genes from "target"
                "anatomy_names": {"$addToSet": "$target"} # Fix: Get anatomy from "target"
            }
        },
        {
            "$lookup": {
                "from": "nodes",
                "localField": "compound_names",
                "foreignField": "id",
                "as": "compound_array",
                "pipeline": [{"$match": {"kind": "Compound"}}]
            }
        },
        {
            "$lookup": {
                "from": "nodes",
                "localField": "disease_names",
                "foreignField": "id",
                "as": "disease_array",
                "pipeline": [{"$match": {"kind": "Disease"}}]
            }
        },
        {
            "$lookup": {
                "from": "nodes",
                "localField": "gene_names",
                "foreignField": "id",
                "as": "gene_array",
                "pipeline": [{"$match": {"kind": "Gene"}}]
            }
        },
        {
            "$lookup": {
                "from": "nodes",
                "localField": "anatomy_names",
                "foreignField": "id",
                "as": "anatomy_array",
                "pipeline": [{"$match": {"kind": "Anatomy"}}]
            }
        },
        {
            "$project": {
                "_id": 0,
                "compound_names": {"$map": {"input": "$compound_array", "as": "el", "in": "$$el.name"}},
                "disease_names": {"$map": {"input": "$disease_array", "as": "el", "in": "$$el.name"}},
                "gene_names": {"$map": {"input": "$gene_array", "as": "el", "in": "$$el.name"}},
                "anatomy_names": {"$map": {"input": "$anatomy_array", "as": "el", "in": "$$el.name"}}
            }
        }
    ]

    results = collection.aggregate(pipeline)
    result_list = []

    for result in results:
        result_entry = []
        if result.get("disease_names"):
            result_entry.append(f"Disease name: {', '.join(result['disease_names'])}")
        if result.get("compound_names"):
            result_entry.append(f"Compounds: {', '.join(result['compound_names'])}")
        if result.get("gene_names"):
            result_entry.append(f"Genes: {', '.join(result['gene_names'])}")
        if result.get("anatomy_names"):
            result_entry.append(f"Anatomy: {', '.join(result['anatomy_names'])}")
        result_list.append("\n".join(result_entry))
    
    print("\n\n".join(result_list))

    #print(result_list)




def main():
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    
    # Deletes all documents in the collection 'nodes' or 'edges'
    # del_collection('nodes')
    # del_collection('edges')

    # Insert nodes and edges (Do not run this since we did it already)
    # insert_nodes()
    # insert_edges()

    # Perform query 1
    query_one()

if __name__ == '__main__':
    main()


