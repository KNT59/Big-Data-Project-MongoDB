from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import csv
import os 
import argparse

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
db_name = "helio_net_rev"
database = client[db_name]
edges_name='edges'
nodes_name= 'nodes'
edges_db = database[edges_name]
nodes_db = database[nodes_name]

node_file = "nodes.tsv"
edge_file = "edges.tsv"

def prepare_nodes():
    nodes = {}
    # node_file = "sample_nodes.tsv"
    with open(node_file, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        #skip the header row 
        next(reader, None)

        for row in reader:
            id, name, kind = row[0], row[1], row[2]
            document_to_insert = {
                'id': id, 
                'name': name,
                'kind': kind,
                'edges_in': [],
                'edges_out': []
            }
            nodes[id] = document_to_insert # The id is an index 
    print("node preparation complete")
    return nodes

def insert_edges_with_node(nodes):
    # edge_file = "sample_edges.tsv"
    collection_name = "nodes"
    collection = database[collection_name]

    with open(edge_file, 'r') as f:
            reader = csv.reader(f, delimiter='\t')
            #skip the header row 
            next(reader, None)

            for row in reader:
                source, metaedge, target = row[0], row[1], row[2]
                # Each edge updates two nodes
                # AuG
                source_node_obj = nodes[source]
                source_node_obj['edges_out'].append({'target':target, 'metaedge':metaedge})

                target_node_obj = nodes[target]
                target_node_obj['edges_in'].append({'source':source, 'metaedge':metaedge})

    # Contains the preprocessed objects prior to insertion
    insert_docs = []
    for key in nodes:
        document = nodes[key]
        insert_docs.append(document)
    collection.insert_many(insert_docs)
                
# Query 1
def query_one(disease_id):
    collection = nodes_db
    #disease_id = "Disease::DOID:0050425"
    pipeline = [
        {
            "$match": 
            {
                "$or": [
                    # Condition 1: Compound kind with edges_out target as disease_id and metaedge in ['treat', 'palliate']
                    {
                        "kind": "Compound",
                        "edges_out": {
                            "$elemMatch": {
                                "target": disease_id,
                                "metaedge": { "$in": ["CtD", "CpD"] }
                            }
                        }
                    },
                    # Condition 2: Gene kind with edges_in source as disease_id and metaedge as 'associate'
                    {
                        "kind": "Gene",
                        "edges_in": {
                            "$elemMatch": {
                                "source": disease_id,
                                "metaedge": "DaG"
                            }
                        }
                    },
                    # Condition 3: Anatomy kind with edges_in source as disease_id and metaedge as 'localizes'
                    {
                        "kind": "Anatomy",
                        "edges_in": {
                            "$elemMatch": {
                                "source": disease_id,
                                "metaedge": "DlA"
                            }
                        }
                    },
                    # Condition 4: Disease name
                    {
                        "id": disease_id
                    }
                ]
            },
        },
        {
            "$project": {
                "_id": 0,  # Exclude the default _id field
                "compound_name": {
                    "$cond": [
                        { "$eq": ["$kind", "Compound"] },
                        "$name",  # Assuming the compound name is stored in the `name` field
                        None
                    ]
                },
                "gene_name": {
                    "$cond": [
                        { "$eq": ["$kind", "Gene"] },
                        "$name",  # Assuming the gene name is stored in the `name` field
                        None
                    ]
                },
                "disease_name": {
                    "$cond": [
                        { "$eq": ["$id", disease_id] },
                        "$name",  # Assuming the disease name is stored in the `name` field
                        None
                    ]
                },
                "anatomy_name": {
                    "$cond": [
                        { "$eq": ["$kind", "Anatomy"] },
                        "$name",  # Assuming the anatomy name is stored in the `name` field
                        None
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": None,  # Group all documents together (null means no grouping key)
                "compounds": { "$push": "$compound_name" },  # Group all compound names together
                "genes": { "$push": "$gene_name" },  # Group all gene names together
                "diseases": { "$push": "$disease_name" },  # Group all disease names together
                "anatomies": { "$push": "$anatomy_name" }  # Group all anatomy names together
            }
        },
        {
            "$project": {
                "_id": 0,  # Exclude the _id field from the final result
                "compounds": { "$filter": { "input": "$compounds", "as": "item", "cond": { "$ne": ["$$item", None] } } },  # Filter out None values
                "genes": { "$filter": { "input": "$genes", "as": "item", "cond": { "$ne": ["$$item", None] } } },  # Filter out None values
                "diseases": { "$filter": { "input": "$diseases", "as": "item", "cond": { "$ne": ["$$item", None] } } },  # Filter out None values
                "anatomies": { "$filter": { "input": "$anatomies", "as": "item", "cond": { "$ne": ["$$item", None] } } }  # Filter out None values
            }
        }
    ]
    results = collection.aggregate(pipeline)
    for r in results:
        print("Disease: ", r['diseases'])
        output =("Disease: ", r['diseases'])
        print("Compounds: ", r['compounds'])
        output += ("Compounds: ", r['compounds'])
        print("Genes: ", r['genes'])
        output += ("Genes: ", r['genes'])
        print("Anatomies: ", r['anatomies'])
        output += ("Anatomies: ", r['anatomies'])

    return output


def query_two():
    collection = nodes_db  # Using nodes collection for the query
    
    pipeline = [
        # Step 1: Match compounds excluding 'palliate' and 'treat' metaedges, and either 'upregulates gene' or 'downregulates gene'
        {"$match": {
            "kind": "Compound",
            "edges_out.metaedge": {"$nin": ["CpD", "CtD"]},  # Exclude 'palliate' and 'treat' metaedges
            "$or": [
                {"edges_out.metaedge": "CuG"},  # 'Upregulates gene'
                {"edges_out.metaedge": "CdG"}   # 'Downregulates gene'
            ]
        }},
        
        # Step 2: Lookup anatomies that have edges_in being localized by a disease
        {"$lookup": {
            "from": "edges",
            "localField": "id",  # Compound ID
            "foreignField": "source",  # Source in edges (Compound → Anatomy)
            "as": "disease_anatomy_edges",
            "pipeline": [
                {"$match": {"metaedge": "DlA"}}  # Disease → Anatomy (localizes)
            ]
        }},
        
        # Step 3: Lookup genes that are either upregulated or downregulated by the compound
        {"$lookup": {
            "from": "edges",
            "localField": "id",  # Compound ID
            "foreignField": "source",  # Source in edges (Compound → Gene)
            "as": "compound_gene_edges",
            "pipeline": [
                {"$match": {
                    "$or": [
                        {"metaedge": "CuG"},  # Upregulates gene
                        {"metaedge": "CdG"}   # Downregulates gene
                    ]
                }}
            ]
        }},
        
        # Step 4: Filter compounds to ensure they have both anatomy and gene edges
        {"$match": {
            "disease_anatomy_edges": {"$ne": []},  # Ensure anatomy edges exist
            "compound_gene_edges": {"$ne": []},  # Ensure gene edges exist
        }},
        
        # Step 5: Project compound names
        {"$project": {
            "_id": 0,  # Exclude the default _id field
            "compound_name": "$name",  # Return compound name
        }}
    ]
    
    # Running the aggregation query
    results = collection.aggregate(pipeline)
    
    output = []
    for result in results:
        output.append(result['compound_name'])
        print("Compound: ", result['compound_name'])
    
    return output

# Test the query
print(query_two())
#query_one("Disease::DOID:0050425")
query_two()
# Parse command-line arguments
parser = argparse.ArgumentParser(description="Run MongoDB Queries")
parser.add_argument("-q1", action="store_true", help="Run Query 1 (requires -id)")
#parser.add_argument("-q2", action="store_true", help="Run Query 2")
parser.add_argument("-id", type=str, help="Disease ID for Query 1")

args = parser.parse_args()

# Execute the selected query
if args.q1 and args.id:
    query_one(args.id)
#if args.q2:
#   query_two()
else:
    print("Usage: Helio_rev.py -q1 -id <disease_id> OR Helio_rev.py -q2")



