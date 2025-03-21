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
                "_id": None,  # Group all documents together (None means no grouping key)
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
        # output += ("Disease: ", r['diseases'])
        print("Compounds: ", r['compounds'])
        # output += ("Compounds: ", r['compounds'])
        print("Genes: ", r['genes'])
        # output += ("Genes: ", r['genes'])
        print("Anatomies: ", r['anatomies'])

def query_two():
    collection_name = 'edges'
    collection = database[collection_name]

    # Construct the aggregation pipeline
    pipeline = [
    # Step 1: Match documents where "kind" is "Gene"
    {
        "$match": {
            "kind": "Gene"
        }
    },
    # Step 2: Add Fields - Filter "edges_in" based on "metaedge" values and get the first match
    {
        "$addFields": {
            "pairA": {
                "$filter": {
                    "input": "$edges_in",  # Array to filter
                    "as": "edge",          # Alias for each object in the array
                    "cond": {
                        "$or": [  # Combine conditions with $or
                            { "$eq": ["$$edge.metaedge", "CuG"] },  # Check if metaedge == "CuG"
                            { "$eq": ["$$edge.metaedge", "CdG"] }
                        ]
                    }
                }
            },
            "pairB": {
                "$filter": {
                    "input": "$edges_in",  # Array to filter
                    "as": "edge",          # Alias for each object in the array
                    "cond": {
                        "$or": [  # Combine conditions with $or
                            { "$eq": ["$$edge.metaedge", "AdG"] },  # Check if metaedge == "AdG"
                            { "$eq": ["$$edge.metaedge", "AuG"] }
                        ]
                    }
                }
            }
        }
    },
    # Step 3: Add matched names based on conditions between pairA and pairB
    {
        "$addFields": {
            "matchedNames": {
                "$map": {
                    "input": "$pairA",  # Iterate over pairA (filtered edges_in)
                    "as": "inEdge",     # Alias for each edge in pairA
                    "in": {
                        "$cond": {
                            "if": {
                                "$eq": ["$$inEdge.metaedge", "AdG"]
                            },
                            "then": {
                                "$let": {
                                    "vars": {
                                        "adgEdge": {
                                            "$arrayElemAt": [
                                                {
                                                    "$filter": {
                                                        "input": "$pairB",  # pairB is already filtered
                                                        "as": "outEdge",
                                                        "cond": { "$eq": ["$$outEdge.metaedge", "CuG"] }
                                                    }
                                                },
                                                0  # Get the first matching element from pairB
                                            ]
                                        }
                                    },
                                    "in": {
                                        "$cond": {
                                            "if": { "$ne": ["$$adgEdge", None] },  # If a match is found in pairB
                                            "then": "$$inEdge.source",  # Push name of the inEdge to array
                                            "else": None  # If no match found, return None
                                        }
                                    }
                                }
                            },
                            "else": {
                                "$cond": {
                                    "if": { "$eq": ["$$inEdge.metaedge", "CdG"] },
                                    "then": {
                                        "$let": {
                                            "vars": {
                                                "augEdge": {
                                                    "$arrayElemAt": [
                                                        {
                                                            "$filter": {
                                                                "input": "$edges_out",  # edges_out array
                                                                "as": "outEdge",
                                                                "cond": { "$eq": ["$$outEdge.metaedge", "AuG"] }
                                                            }
                                                        },
                                                        0  # Get the first matching element from edges_out
                                                    ]
                                                }
                                            },
                                            "in": {
                                                "$cond": {
                                                    "if": { "$ne": ["$$augEdge", None] },  # If a match is found in edges_out
                                                    "then": "$$inEdge.source",  # Push name of the inEdge to array
                                                    "else": None  # If no match, return None
                                                }
                                            }
                                        }
                                    },
                                    "else": None  # If no conditions met, return None
                                }
                            }
                        }
                    }
                }
            }
        }
    },
    {
        "$addFields": {
            "matchedNames": {
                "$filter": {
                    "input": "$matchedNames",  # Array to filter
                    "as": "name",              # Alias for each element in matchedNames
                    "cond": { "$ne": ["$$name", None] }  # Condition to remove null values
                }
            }
        }
    },
    {
        "$addFields": {
            "matchedNames": {
                "$setUnion": [
                    "$matchedNames",  # The matchedNames array
                    []                # An empty array to "set" the unique elements from matchedNames
                ]
            }
        }
    },
    # Step 4: Project matchedNames field
    {
        "$project": {
            # "matchedNames": 1  # Only include matchedNames in the output
            'matchedNames': 1
        }
    }
]
  

    results = collection.aggregate(pipeline)
    print(results)
    i = 1
    for r in results:
        compounds = r['matchedNames']
        for c in compounds:
            print(i, ': ', c)
            i +=1
    

# Deletes all documents in the collection
def del_collection(collection_name):
    collection = database[collection_name]
    collection.delete_many({})

#def main():
    # Send a ping to confirm a successful connection
    #try:
        #client.admin.command('ping')
        #print("Pinged your deployment. You successfully connected to MongoDB!")
    #except Exception as e:
        #print(e)
    
    # Deletes all documents in the collection 'nodes' or 'edges'
    # del_collection('nodes')

    # Insert nodes and edges (Do not run this since we did it already)
    # nodes = prepare_nodes()
    # insert_edges_with_node(nodes)

    # Perform query 1
    #query_one("Disease::DOID:9206")


# Parse command-line arguments
parser = argparse.ArgumentParser(description="Run MongoDB Queries")
parser.add_argument("-q1", action="store_true", help="Run Query 1 (requires -id)")
#parser.add_argument("-q2", action="store_true", help="Run Query 2")
parser.add_argument("-id", type=str, help="Disease ID for Query 1")

args = parser.parse_args()

query_two()
# Execute the selected query
# if args.q1 and args.id:
#     query_one(args.id)
# #if args.q2:
# #   query_two()
# else:
#     print("Usage: Helio_rev.py -q1 -id <disease_id> OR Helio_rev.py -q2")



