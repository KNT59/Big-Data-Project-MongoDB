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
db_name = "helio_net_rev"
database = client[db_name]

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
def query_one():
    collection_name = 'nodes'
    collection = database[collection_name]

    disease_id = "Disease::DOID:0050425"
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
        print("Compounds: ", r['compounds'])
        print("Genes: ", r['genes'])
        print("Anatomies: ", r['anatomies'])


def query_two():
    collection_name = 'edges'
    collection = database[collection_name]

    # Construct the aggregation pipeline
    pipeline = [
        # Step 1: Match conditions for filtering
        {
            "$match": {
                "$or": [
                    # Include compounds if it does not directly treat/palliate disease
                    # and can upregulates/downregulates genes
                    {
                        "kind": "Compound",
                        "$and": [
                            {
                                "edges_out": {
                                    "$not": {
                                        "$elemMatch": { 
                                            "metaedge": { "$in": ["CtD", "CpD"] }
                                        }
                                    }
                                }
                            },
                            {
                                "edges_out": {
                                    "$elemMatch": {
                                        "metaedge": { "$in": ["CuG", "CdG"] }
                                    }
                                }
                            }
                        ]
                    },
                    # Include anatomies with upregulate or downregulate in edges_out
                    {
                        "kind": "Anatomy",
                        "edges_out": {
                            "$elemMatch": {
                                "metaedge": { "$in": ["AuG", "AdG"] }
                            }
                        }
                    },
                ]
            }
        },
        # Add conditional checks for upregulate, downregulate conditions and merge compounds
        {
            "$addFields": {
                "compound_up_anatomy_down": {
                    "$cond": [
                        {
                            "$and": [
                                { "$eq": ["$compound_name", "upregulate"] },  # Compound upregulate
                                { "$eq": ["$anatomy_name", "downregulate"] }  # Anatomy ownregulate
                            ]
                        },
                        { "$in": [ "$disease_id", "$edges_in.source" ] },  # Check if disease localizes to anatomy
                        False
                    ]
                },
                "compound_anatomy_condition_reverse": {
                    "$cond": [
                        {
                            "$and": [
                                { "$eq": ["$compound_name", "downregulate"] },  # Compound is downregulate
                                { "$eq": ["$anatomy_name", "upregulate"] }  # Anatomy is upregulate
                            ]
                        },
                        { "$in": [ "$disease_id", "$edges_in.source" ] },  # Check if disease localizes to anatomy
                        False
                    ]
                }
            }
        },

        # Step 4: Merge all the relevant compounds based on the conditions above
        {
            "$project": {
                "_id": 0,
                "merged_compounds": {
                    "$concatArrays": [
                        { "$cond": [{ "$ne": ["$compound_anatomy_condition", False] }, ["$compound_name"], []] },
                        { "$cond": [{ "$ne": ["$compound_anatomy_condition_reverse", False] }, ["$compound_name"], []] }
                    ]
                }
            }
        }
    ]
    

# Deletes all documents in the collection
def del_collection(collection_name):
    collection = database[collection_name]
    collection.delete_many({})

def main():
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    
    # Deletes all documents in the collection 'nodes' or 'edges'
    # del_collection('nodes')

    # Insert nodes and edges (Do not run this since we did it already)
    # nodes = prepare_nodes()
    # insert_edges_with_node(nodes)

    # Perform query 1
    query_one()

if __name__ == '__main__':
    main()


