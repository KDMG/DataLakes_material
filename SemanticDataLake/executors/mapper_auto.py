import time
import pandas as pd
from datasketch import MinHashLSHEnsemble, MinHash

# Parameters
NUM_PERM = 256
NUM_PART = 32
THRESHOLD = 0.8


def initialize_lsh(kg):
    # Create an LSH Ensemble index with threshold and number of partition settings.
    lshensemble = MinHashLSHEnsemble(threshold=THRESHOLD, num_perm=NUM_PERM, num_part=NUM_PART)

    # Initialize LSHEnsemble
    index = []
    dimensions = kg.get_dimensions()
    # Extract all levels and create MinHash for each
    for dim in dimensions:
        levels = kg.get_levels(dim)
        for lev in levels:
            members = kg.get_members_from_level(lev, fragmentOutput=True)

            # Create the MinHash for the i-th level
            m = MinHash(num_perm=NUM_PERM)
            m.update_batch([s.encode('utf8') for s in members])
            index.append(tuple((lev, m, len(members))))
    # Pack all together
    lshensemble.index(index)
    return lshensemble


def map_source_domain(values, lshensemble):
    # Hashing the dataset column
    m = MinHash(NUM_PERM)
    values_set = set(values)
    m.update_batch([s.encode('utf8') for s in values_set])
    mappings = lshensemble.query(m, len(values_set))
    return mappings


def calculate_profile(values, members):
    frequency = dict()
    idx = pd.Index(values)
    count = idx.value_counts()
    members_list = members
    for m in members_list:
        m_fragment = m[m.rfind('/') +1:]
        if m_fragment in count:
            frequency[m] = count[m_fragment]
    delta = len(count) - len(frequency)
    if delta > 0:
        frequency["other"] = delta
    return frequency
