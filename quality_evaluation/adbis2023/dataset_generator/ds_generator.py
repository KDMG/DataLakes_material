import math
import time
from rdflib import Namespace
from models.KG import KG
import random
import pandas as pd
import numpy as np

DATASET_FOLDER = "datasets/" # set here your folder
GRAPH_FOLDER = "kg/" #set here your folder
GRAPH = "knowledge_graph_D10_L5_10.ttl"
NUM_ROWS = [1000000] # add other values in the list 
NUM_COLS = [20] # add other values in the list
PERC_DIMENSIONS = 0.2 # the percentage of columns that will be generated as mapped to level in the KG
PERC_NOISE = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

ns_project = Namespace("http://kdmg.dii.univpm.it/test/")
ns_kpionto = Namespace("http://w3id.org/kpionto/")

kGraph = None


def pick_level(levels):
    return random.choice(levels)

# The method generates a dataset according to the input parameters
def generate_ds(num_righe, num_colonne, perc_dimensioni, perc_rumore, kg):
    num_dimensioni = math.floor(num_colonne * perc_dimensioni)
    num_attributi = num_colonne - num_dimensioni
    all_dimensions = kg.get_dimensions()

    # Initialize DataFrame
    df = pd.DataFrame()
    # Generate dimension fields
    picked_dimensions = random.sample(all_dimensions, num_dimensioni)

    print("Generating dimensions...")
    for d in picked_dimensions:
        # Pick a level
        level = pick_level(kg.get_levels(d))
        # Generate members and add noise
        members = kg.get_members_from_level(level, fragmentLevel=False, fragmentOutput=True)
        #members = sorted(members)[:len(members)//2] # pick only 50% of members
        noise = ['x']
        num_rumore = math.floor(num_righe * perc_rumore)
        num_valori = num_righe - num_rumore
        list_values = random.choices(members, k=num_valori)
        list_noise = ['x'] * num_rumore #generates the noise elements
        all_values = list_values + list_noise
        # Shuffle values and add the column
        np.random.shuffle(all_values)
        df[level] = all_values

    print("Generating attributes...")
    # Generate attribute fields
    counter = 0
    for attr in range(0, num_attributi):
        num_distinct_values = random.choice(range(1, num_righe + 1, math.floor(num_righe / 10)))
        values = np.random.randint(0, num_distinct_values, size=num_righe)
        # Add the column
        df["attr" + str(counter)] = values
        counter += 1

    print("Shuffling and saving...")
    # Shuffle columns
    df = df[np.random.default_rng(seed=42).permutation(df.columns.values)]

    return df

def main():
    print("Let's start generating a dataset")
    global kGraph
    print("Importing the Knowledge Graph...")
    kGraph = KG(GRAPH_FOLDER+GRAPH)
    counter = 0
    for num_cols in NUM_COLS:
        subfolder =""
        for num_rows in NUM_ROWS:
            for noise in PERC_NOISE:
                perc_noise = noise / 100.0
                start = time.time()
                df = generate_ds(num_rows, num_cols, PERC_DIMENSIONS, perc_noise, kGraph)
                filename = DATASET_FOLDER+subfolder+"ds_C"+str(num_cols)+"_R"+str(num_rows)+"_n"+str(noise)+".csv"
                df.to_csv(filename, sep=',')
                print("\nAll done.\nRunning time: ", time.time() - start, " seconds")
        counter += 1

if __name__ == "__main__":
    main()

