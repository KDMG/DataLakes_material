import time
import datetime
import pandas as pd
from datasketch import MinHashLSHEnsemble, MinHash
import random
import numpy as np
from rdflib.plugins.sparql.parserutils import value

from models.KG import KG

DATASET_FOLDER = "../datasets/join_experiment/"
GRAPH_FOLDER = "../kg/"
GRAPH = "knowledge_graph_D10_L5_10.ttl"

NUM_COLS = [2, 4, 6, 8, 10]
NUM_ROWS = [1000000]
#NUM_ROWS_TO_CHOOSE = [1000,10000,100000,1000000]
NUM_ROWS_TO_CHOOSE = [1000000]
PERC_NOISE = 0
NUM_REPETITIONS = 10

# Parameters
NUM_PERM = 128
NUM_PART = 32
THRESHOLD = 0.8

#NOISE_PERC = 0.2
VARIABLE_NOISE_PERC = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
COMBINED = True

def initialize_lsh(kg):
    print("Generating MinHashes for dimension levels...")
    # Create an LSH Ensemble index with threshold and number of partition settings.
    lshensemble = MinHashLSHEnsemble(threshold=THRESHOLD, num_perm=NUM_PERM, num_part=NUM_PART)

    # Initialize LSHEnsemble
    index = []
    dimensions = kg.get_dimensions()
    # Extract all levels and create MinHash for each
    for dim in dimensions:
        levels = kg.get_levels(dim)
        for lev in levels:
            members = kg.get_members_from_level(lev)

            # Create the MinHash for the i-th level
            m = MinHash(num_perm=NUM_PERM)
            m.update_batch([s.encode('utf8') for s in members])
            index.append(tuple((lev, m, len(members))))
    # Pack all together
    lshensemble.index(index)
    return lshensemble

def generate_ds(num_rows, num_columns, perc_rumore, kg):
    all_dimensions = kg.get_dimensions()
    # Initialize DataFrame
    df = pd.DataFrame()
    # Pick 1 dimension
    picked_dimensions = random.sample(all_dimensions, num_columns)

    print("Generating dimensions...")
    # Put the last level
    rows_to_create = int(num_rows * 1.1)
    for dim in picked_dimensions:
        all_levels = kg.get_levels(dim)
        # Pick the last level of the picked dimension
        level = all_levels[4]
        # Generate members and add noise
        members = kg.get_members_from_level(level)

        list_values = random.choices(members, k=rows_to_create)
        # Shuffle values and add the column
        np.random.shuffle(list_values)
        df[level] = list_values

    # Removing duplicates
    df.drop_duplicates()
    delta_rows = df.shape[0] - num_rows
    if delta_rows > 0:
        df = df.tail(-delta_rows)
    else:
        raise Exception

    print("Shuffling and saving...")
    # Shuffle columns
    df = df[np.random.default_rng(seed=42).permutation(df.columns.values)]

    return df


def join_test(s1, rows_S1, directory):
    print("\t1. Loading DataFrame for S1...")

    dimension_cols = []
    for c in s1.columns:
        dimension_cols.append(c)

    if COMBINED:
        print("\t2. Calculating combined MinHash for S1...")
        # 2. Combined MinHashing of the schema of S1
        combined_s1 = MinHash(NUM_PERM)
        df_combined_s1 = s1[dimension_cols[0]].map(str)
        for col in dimension_cols[1:]:
            df_combined_s1 = df_combined_s1 + "_" + s1[col].map(str)
        values_set_s1 = set(df_combined_s1.tolist())
        combined_s1.update_batch([s.encode('utf8') for s in values_set_s1])

    print("\t3. Generation of Dataframes for S2...")
    s2 = s1.copy(deep=True)

    for rows_S2 in NUM_ROWS_TO_CHOOSE:
        for nnn in VARIABLE_NOISE_PERC:
            if rows_S2 <= rows_S1:
                print("\t\tCreating S2 with ",rows_S2," rows")
                # Get a subset of rows and shuffle
                s2_ = s2.head(rows_S2)
                s2_.is_copy = False

                # 4. Noise injection
                num_noisy_values = int(rows_S2 *nnn)
                print(num_noisy_values)
                list_noise = ['x'] * num_noisy_values  # generates the noise elements
                shortened_col = s2_[dimension_cols[0]].head(rows_S2-num_noisy_values)
                print(len(shortened_col))
                # Update the column
                s2_ = s2_.drop(columns=[dimension_cols[0]])
                s2_.insert(0,dimension_cols[0], shortened_col.tolist() + list_noise)
                s2_.reset_index(inplace=True)  # first block is the same, the rest is noise

                # Shuffle rows
                s2_ = s2_.sample(frac=1, random_state=1)

                # 5. Combined MinHash for S2
                if COMBINED:
                    combined_s2 = MinHash(NUM_PERM)
                    df_combined_s2 = s2_[dimension_cols[0]].map(str)
                    for col in dimension_cols[1:]:
                        df_combined_s2 = df_combined_s2 + "_" + s2_[col].map(str)
                    values_set_s2 = set(df_combined_s2.tolist())
                    combined_s2.update_batch([s.encode('utf8') for s in values_set_s2])

                #6. Calculation of join
                print("\t\tCalculating join and JI between S1 and S2....")
                try:
                    start_join = time.time()
                    result = pd.merge(s1, s2_, on=dimension_cols, how='inner')
                    end_join = time.time() - start_join
                    result_size = result.shape[0]
                except Exception as e:
                    print(e)
                    end_join = -1.0
                    result_size = -1

                #7. Calculation of joinability index
                start_ji = time.time()
                ji = calculate_ji(combined_s1,rows_S1,combined_s2,rows_S2)
                end_ji = time.time() - start_ji

                #start_inter = time.time()
                #cs = containment_set(combined_s1, combined_s2, rows_S2)
                #end_inter = time.time() - start_inter

                #8. Alt: intersection among sets
                """start_inter = time.time()
                cs = containment_set(values_set_s1, values_set_s2)*rows_S2
                end_inter = time.time() - start_inter
    
                start_inter_alt = time.time()
                cs_alt = containment_set_alt(s1, s2_, s1.count(), s2_.count())*rows_S2
                end_inter_alt = time.time() - start_inter_alt"""

                with open(directory + "test_paper_noise.log", 'a') as f:
                    f.write(str(len(s1.columns)) + ";" + str(rows_S1) + ";" + str(rows_S2) + ";" + str(nnn) + ";" +
                    str('{0:.3f}'.format(end_join)).replace('.', ',') + ";" + str(result_size) + ";" +
                    str('{0:.3f}'.format(end_ji)).replace('.', ',') + ";" + str(int(ji * rows_S2)) + "\n")
                    #str('{0:.3f}'.format(end_inter)).replace('.', ',') + ";" + str(cs) + ";" +
                    #str('{0:.3f}'.format(end_inter_alt)).replace('.', ',') + ";" + str(cs_alt) + "\n")



def calculate_ji(combined_s1, size_s1, combined_s2, size_s2):
    stop = False
    first = 0.0
    last = 1.0
    delta = 0.049
    ji = 0
    # The termination criteria for the dychotomic search is that we got a delta <0.05
    while first <= (last - delta):
        found = False
        q = (first + last) / 2
        lshensemble = MinHashLSHEnsemble(threshold=q, num_perm=NUM_PERM, num_part=NUM_PART)
        lshensemble.index([("s1", combined_s1, size_s1)])
        for k in lshensemble.query(combined_s2, size_s2):
            print(k)
            found = True
            ji = q
        if (found):
            first = q
        else:
            last = q

    #print("Result:\n\tsources joinable with containment > " + str(sure))
    return ji

def main():
    print("Let's start generating a dataset")
    global kg
    print("Importing the Knowledge Graph...")
    kg = KG(GRAPH_FOLDER + GRAPH)
    lshensemble = initialize_lsh(kg)

    with open(DATASET_FOLDER + "test_paper_noise.log", 'a') as f:
        f.write("Produced on: " + str(datetime.datetime.now()) + "\n")
        f.write("columns;rows_S1;rows_S2;noise;join;ji;card_join;card_ji\n")

    for num_cols in NUM_COLS:
        for num_rows in NUM_ROWS:
            for i in range(0,NUM_REPETITIONS):
                # Generate dataset
                print("Generating S1 with ",num_rows," rows...")
                # Generate dataset with only dimensional columns
                df = generate_ds(num_rows, num_cols, PERC_NOISE, kg)
                join_test(df, num_rows, DATASET_FOLDER)


def containment_set(s1,s2):
        cs = len(np.intersect1d(s1,s2))/len(s2)
        return cs

def containment_set_alt(s1,s2,size_s1,size_s2):
    union_sketch = s1.copy()
    union_sketch.merge(s2)
    union = union_sketch.count()
    # From Inclusion–exclusion principle: |A ∩ B| = |A| + |B| - |A ∪ B|
    inter = size_s1 + size_s2 - union
    jaccard = inter / union
    return inter / size_s2

if __name__ == "__main__":
    main()
