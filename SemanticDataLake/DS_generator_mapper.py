import math
import time
from rdflib import Namespace
from models.KG import KG
import random
import pandas as pd
import numpy as np
import datetime
import executors.mapper as mapper

DATASET_FOLDER = "../datasets/mapping_time_noise_10fold/"
GRAPH_FOLDER = "../kg/"
GRAPH = "knowledge_graph_D10_L5_10.ttl"
#NUM_ROWS = [1000,10000,100000,1000000]
NUM_ROWS = [10000000]
#NUM_COLS = [10, 20, 30, 40, 50] # 2,4,6,8,10
NUM_COLS =  [30]
PERC_DIMENSIONS = 0.2
#PERC_NOISE = [0]
PERC_NOISE = [0,10,20,30,40,50,60,70,80,90]

ns_project = Namespace("http://kdmg.dii.univpm.it/test/")
ns_kpionto = Namespace("http://w3id.org/kpionto/")

kg = None


def pick_level(levels):
    return random.choice(levels)


def generate_ds(num_righe, num_colonne, perc_dimensioni, perc_rumore):
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
        members = kg.get_members_from_level(level)
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
    global kg
    print("Importing the Knowledge Graph...")
    kg = KG(GRAPH_FOLDER+GRAPH)
    lshensemble = mapper.initialize_lsh(kg)

    with open(DATASET_FOLDER + "test.log", 'a') as f:
        f.write("Produced on: " + str(datetime.datetime.now()) + "\n")
        f.write("filename;rows;columns;dimensions;noise;" +
                "sum_time_hashing;avg_time_hashing;std_time_hashing;" +
                "avg_time_hashing_dim;std_time_hashing_dim;" +
                "avg_time_hashing_attr;std_time_hashing_attr;" +
                "sum_time_query;avg_time_query;std_time_query;" +
                "combined_time;profile_time;end_to_end_time;" +
                "effectiveness\n")
    for num_cols in NUM_COLS:
        for num_rows in NUM_ROWS:
            for noise in PERC_NOISE:
                perc_noise = noise / 100.0
                for it_number in range(0,10): #10 iterations
                    df = generate_ds(num_rows, num_cols, PERC_DIMENSIONS, perc_noise)
                    mapper.map_source(df, num_rows, num_cols, noise, "", DATASET_FOLDER, lshensemble, kg)

if __name__ == "__main__":
    main()
