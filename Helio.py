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
    # Create the aggregation pipeline
    pipeline = [
        {
            # Filter INPUT DOCUMENTS
           "$match": {
                "$or": [
                    {
                        "$and": [
                            # ((metaedge == "CtD" OR metaedge == "CpD") AND target == disease_id)
                            { "metaedge": { "$in": ["CtD", "CpD"] } },  
                            { "target": disease_id }         
                        ]
                    },
                    # OR
                    {
                        "$and": [
                            # (metagedge == 'DaG' AND source == disease_id)
                            { "metaedge": "DaG" },                     
                            { "source": disease_id }                       
                        ]
                    },
                    # OR
                    {
                       "$and": [
                            # (metagedge == 'DlA' AND source == disease_id)
                            { "metaedge": "DlA" },                     
                            { "source": disease_id }                       
                        ]  
                    }
                ]
            }
        },
        { 
            # Perform a join to the 'nodes' collection based on matching ids (this gets all compound nodes)
            "$lookup": {
                "from": "nodes",    # The collection to join to
                "localField": "source", # The field of input document for joining
                "foreignField": "id",   # Field in document in the collection specified by "from" as the key to join 
                "as": "compound_array", # Joined documents from the collection specified by "from" will be added to this array and be passed to next stage
                "pipeline": [
                    {
                        "$match": {
                            "kind": "Compound"
                        }
                    }
                ]
            }
        },
        {
            # Perform another join to 'nodes' collection based on matching ids (this gets the specified disease node)
            "$lookup": {
                "from": "nodes",    # The collection to join to
                "localField": "target", # The field of input document for joining
                "foreignField": "id",   # Field in document in the collection specified by "from" as the key to join 
                "as": "disease_array", # Joined documents from the collection specified by "from" will be added to this array and be passed to next stage
                "pipeline": [
                    {
                        "$match": {
                            "kind": "Disease"
                        }
                    }
                ]
            }
        },
        {
            # Perform another join to 'nodes' collection based on matching ids (this gets the specified disease node)
            "$lookup": {
                "from": "nodes",    # The collection to join to
                "localField": "target", # The field of input document for joining
                "foreignField": "id",   # Field in document in the collection specified by "from" as the key to join 
                "as": "gene_array", # Joined documents from the collection specified by "from" will be added to this array and be passed to next stage
                "pipeline": [
                    {
                        "$match": {
                            "kind": "Gene"
                        }
                    }
                ]
            }
        },
        {
            # Perform another join to 'nodes' collection based on matching ids (this gets the specified disease node)
            "$lookup": {
                "from": "nodes",    # The collection to join to
                "localField": "target", # The field of input document for joining
                "foreignField": "id",   # Field in document in the collection specified by "from" as the key to join 
                "as": "anatomy_array", # Joined documents from the collection specified by "from" will be added to this array and be passed to next stage
                "pipeline": [
                    {
                        "$match": {
                            "kind": "Anatomy"
                        }
                    }
                ]
            }
        },
        # {
        #     # Limits number of documents passed to the next stage of pipeline or output
        #     "$limit": 5
        # },
        # Extract specific output fields
        {
            "$project": {
                "_id": 0,
                "compound_array": {
                    "$map": {
                        "input": "$compound_array",
                        "as": "element",
                        "in": {
                            "name": "$$element.name"
                        }
                    }
                },
                "disease_array": {
                    "$map": {
                        "input": "$disease_array",
                        "as": "element",
                        "in": {
                            "name": "$$element.name"
                        }
                    }
                },
                "gene_array": {
                    "$map": {
                        "input": "$gene_array",
                        "as": "element",
                        "in": {
                            "name": "$$element.name"
                        }
                    }
                },
                "anatomy_array": {
                    "$map": {
                        "input": "$anatomy_array",
                        "as": "element",
                        "in": {
                            "name": "$$element.name"
                        }
                    }
                }         
            }
        }
    ]
    # Execute the query and obtain results
    # The returned type is a CommandCursor object, check the properties and
    # methods here: https://pymongo.readthedocs.io/en/stable/api/pymongo/command_cursor.html
    results = collection.aggregate(pipeline)
    cnt = 0
    for result in results:
        print(result)
        cnt += 1
    print(cnt)


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


