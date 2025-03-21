import tkinter as tk
from tkinter import messagebox
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from Helio_rev import query_one, query_two


def printquery(input):
    result_display.delete(1.0, tk.END)
    result_display.insert("end",str(query_one(input)))


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
query_button = tk.Button(window, text="Run Query 1", command=lambda: printquery(disease_entry.get()), height=2, width=20)
query_button.pack(pady=10)
#run_button = tk.Button(window, text="Get New Drugs", command=run_query, height=2, width=20)
#run_button.pack(pady=10)

# Results display area
result_display = tk.Text(window, height=10, width=70)
result_display.pack(pady=10)


# Start the Tkinter event loop
window.mainloop()
