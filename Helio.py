import tkinter as tk
from tkinter import messagebox
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os
import argparse
import threading

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
def query_one(disease_id):
    collection_name = 'edges'
    collection = database[collection_name]
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
    output = "\n\n".join(result_list)
    print("\n\n".join(result_list))
    # Clear the previous content in the Text widget
    result_display.delete(1.0, tk.END)

    # Insert the new result data into the Text widget
    result_display.insert(tk.END, output)

    #print(result_list)
#query 2
def run_query():
    collection_edges = database["edges"]
    collection_nodes = database["nodes"]

    pipeline = [
        {
            "$match": {"metaedge": "GiG"}  # Interactions between genes
        },
        {
            "$lookup": {
                "from": "nodes",
                "localField": "source",
                "foreignField": "id",
                "as": "source_node"
            }
        },
        {
            "$lookup": {
                "from": "nodes",
                "localField": "target",
                "foreignField": "id",
                "as": "target_node"
            }
        },
        {
            "$unwind": "$source_node"
        },
        {
            "$unwind": "$target_node"
        },
        {
            "$match": {
                "source_node.kind": "Compound",
                "target_node.kind": "Gene"
            }
        },
        {
            "$group": {
                "_id": "$source_node.id",
                "compound_name": {"$first": "$source_node.name"}
            }
        }
    ]

    results = list(collection_edges.aggregate(pipeline))
    
    if results:
        compound_names = "\n".join(result["compound_name"] for result in results)
    else:
        compound_names = "No new drugs found."

    # Clear previous results
    result_display.delete(1.0, tk.END)
    result_display.insert(tk.END, compound_names)
# Main GUI window
window = tk.Tk()
window.title("MongoDB GUI")
window.geometry("600x400")

# Disease ID input field
disease_label = tk.Label(window, text="Enter Disease ID:")
disease_label.pack(pady=10)

disease_entry = tk.Entry(window, width=50)
disease_entry.pack(pady=5)

# Query button
query_button = tk.Button(window, text="Run Query 1", command=lambda: query_one(disease_entry.get()), height=2, width=20)
query_button.pack(pady=10)
run_button = tk.Button(window, text="Get New Drugs", command=run_query, height=2, width=20)
run_button.pack(pady=10)

# Results display area
result_display = tk.Text(window, height=10, width=70)
result_display.pack(pady=10)

# Start the Tkinter event loop
window.mainloop()

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Run MongoDB Queries")
parser.add_argument("-q1", action="store_true", help="Run Query 1 (requires -id)")
parser.add_argument("-q2", action="store_true", help="Run Query 2")
parser.add_argument("-id", type=str, help="Disease ID for Query 1")

args = parser.parse_args()

# Execute the selected query
if args.q1 and args.id:
    query_one(args.id)
elif args.q2:
    run_query()
else:
    print("Usage: script.py -q1 -id <disease_id> OR script.py -q2")
