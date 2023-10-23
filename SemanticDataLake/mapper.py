import os
import time
import datetime
import statistics
import pandas as pd
from datasketch import MinHashLSHEnsemble, MinHash
from models.KG import KG

COMPUTE_PROFILE = True
DATASET_FOLDER = "../datasets/"
#FOLDERS = ["noise_experiment_consistent/0/", "noise_experiment_consistent/1/", "noise_experiment_consistent/2/",
#           "noise_experiment_consistent/3/", "noise_experiment_consistent/4/"]
FOLDERS = ["mapping_time_experiment/0/","mapping_time_experiment/1/","mapping_time_experiment/2/","mapping_time_experiment/3/","mapping_time_experiment/4/"]
GRAPH_FOLDER = "../kg/"
GRAPH = "knowledge_graph_D10_L5_10.ttl"
LOG_DIRECTORY = "../logs/"

# Parameters
NUM_PERM = 128
NUM_PART = 32
THRESHOLD = 0.8


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


def get_info_file(filename):
    items = filename.split('_')
    columns = int(items[1][1:])
    rows = int(items[2][1:])
    noise = int(items[3].split('.')[0][1:])
    return columns, rows, noise

def get_info_from_df(df):
    return 10,10

def get_df_from_filename(directory, filename):
    return pd.read_csv(directory + filename)

def map_source(df, rows, columns, noise, filename, directory, lshensemble, kg):
    start_source = time.time()
    # Read the input file
    print("Reading source...")
    durations_hashing = {}
    duration_hashing_dims = []
    duration_hashing_attr = []
    durations_query = {}
    dimension_cols = []
    profile_time = []
    wasted_time = 0 # time that should not be considered for total execution time

    num_dimensions = 0
    num_correctly_identified = 0
    count = 0
    # For each column of the data source
    for col in df.columns:
        if str(col).startswith('L'):
            num_dimensions = num_dimensions + 1
            dimension_cols.append(col)
        values = df[col].astype('str').tolist()

        # Hashing the dataset column
        m = MinHash(NUM_PERM)
        start_time_hashing = time.time()
        values_set = set(values)
        m.update_batch([s.encode('utf8') for s in values_set])
        dur_hashing = time.time() - start_time_hashing
        if str(col).startswith('L'):
            duration_hashing_dims.append(dur_hashing)
        else:
            duration_hashing_attr.append(dur_hashing)
        durations_hashing[col] = dur_hashing

        # Query
        start_time_query = time.time()
        mappings = lshensemble.query(m, len(values_set))
        durations_query[col] = time.time() - start_time_query

        for mapping in mappings:
            if str(col).startswith('L'):
                if mapping == col:
                    num_correctly_identified = num_correctly_identified + 1
                    if COMPUTE_PROFILE:
                        # Extraction of members from the level: it can be done off-line, so this time is not counted in the total execution time
                        start_members_time = time.time()
                        members = kg.get_members_from_level(col)
                        wasted_time = wasted_time + (time.time() - start_members_time)

                        # Profile time
                        start_profile_time = time.time()
                        profile = calculate_profile(df[col], col, members)
                        end_profile_time = time.time() - start_profile_time
                        profile_time.append(end_profile_time)

                        print("col:",col,"> profile time:",end_profile_time)


    # Calculating combined MinHashes for dimensional schema
    combined = MinHash(NUM_PERM)
    start_time_combined = time.time()
    df_combined = df[dimension_cols[0]].map(str)
    for col in dimension_cols[1:]:
        df_combined = df_combined + "_" + df[col].map(str)
    values_set = set(df_combined.tolist())
    combined.update_batch([s.encode('utf8') for s in values_set])
    time_combined = time.time() - start_time_combined

    end_source = time.time() - start_source - wasted_time

    # Statistics
    sum_durations_hashing = sum(durations_hashing.values())
    avg_durations_hashing = statistics.mean(durations_hashing.values())
    std_durations_hashing = statistics.stdev(durations_hashing.values())

    avg_duration_hashing_dim = statistics.mean(duration_hashing_dims)
    std_duration_hashing_dim = statistics.stdev(duration_hashing_dims)
    if len(duration_hashing_attr)>0:
        avg_duration_hashing_attr = statistics.mean(duration_hashing_attr)
        std_duration_hashing_attr = statistics.stdev(duration_hashing_attr)
    else:
        avg_duration_hashing_attr = -1
        std_duration_hashing_attr = -1

    sum_durations_query = sum(durations_query.values())
    avg_durations_query = statistics.mean(durations_query.values())
    std_durations_query = statistics.stdev(durations_query.values())

    sum_profile_time = sum(profile_time)
    effectiveness = num_correctly_identified * 1.0 / num_dimensions

    with open(directory + "test.log", 'a') as f:
        f.write(filename + ";" + str(rows) + ";" + str(columns) + ";" + str(num_dimensions) + ";" + str(noise) + ";" +
                str('{0:.4g}'.format(sum_durations_hashing)).replace('.', ',') + ";" +
                str('{0:.4g}'.format(avg_durations_hashing)).replace('.', ',') + ";" +
                str('{0:.4g}'.format(std_durations_hashing)).replace('.', ',') + ";" +
                str('{0:.4g}'.format(avg_duration_hashing_dim)).replace('.', ',') + ";" +
                str('{0:.4g}'.format(std_duration_hashing_dim)).replace('.', ',') + ";" +
                str('{0:.4g}'.format(avg_duration_hashing_attr)).replace('.', ',') + ";" +
                str('{0:.4g}'.format(std_duration_hashing_attr)).replace('.', ',') + ";" +
                str('{0:.4g}'.format(sum_durations_query)).replace('.', ',') + ";" +
                str('{0:.4g}'.format(avg_durations_query)).replace('.', ',') + ";" +
                str('{0:.4g}'.format(std_durations_query)).replace('.', ',') + ";" +
                str('{0:.4g}'.format(time_combined)).replace('.', ',') + ";" +
                str('{0:.4g}'.format(sum_profile_time)).replace('.', ',') + ";" +
                str('{0:.4g}'.format(end_source)).replace('.', ',') + ";" +
                str('{0:.2g}'.format(effectiveness)).replace('.', ',') + "\n")

# Input: list dei values, nome del level
def calculate_profile(values, level, members):
    frequency = {}
    x_time = time.time()
    idx = pd.Index(values)
    x2_time = time.time()-x_time
    print("\tIndexing time:",x2_time)
    y_time = time.time()
    count = idx.value_counts()
    print("\tIndex counting time:",(time.time()-y_time))
    z_time = time.time()
    #for name, number in count.items():
    #    if name in members:
    #        frequency[name] = number
    for m in members:
        if m in count:
            frequency[m]=count[m]
    delta = len(count) - len(frequency)
    if delta > 0:
        frequency["other"] = delta
    print("\tCycling time:",(time.time()-z_time))
    return frequency





def main():
    print("Importing the Knowledge Graph...")
    kg = KG(GRAPH_FOLDER + GRAPH)
    lshensemble = initialize_lsh(kg)

    for folder in FOLDERS:
        files = []
        # read the folder
        for x in os.listdir(DATASET_FOLDER + folder):
            if x.endswith(".csv"):
                files.append(x)
        print("Let's start mapping...")

        with open(DATASET_FOLDER + folder + "test.log", 'a') as f:
            f.write("Produced on: " + str(datetime.datetime.now()) + "\n")
            f.write("filename;rows;columns;dimensions;noise;" +
                    "sum_time_hashing;avg_time_hashing;std_time_hashing;" +
                    "avg_time_hashing_dim;std_time_hashing_dim;" +
                    "avg_time_hashing_attr;std_time_hashing_attr;" +
                    "sum_time_query;avg_time_query;std_time_query;" +
                    "combined_time;profile_time;end_to_end_time;" +
                    "effectiveness\n")

        for file in files:
            print(file)
            df = get_df_from_filename(DATASET_FOLDER + folder, file)
            columns, rows, noise = get_info_file(file)
            map_source(df, rows, columns, noise, file, DATASET_FOLDER + folder, lshensemble, kg)


if __name__ == "__main__":
    main()
