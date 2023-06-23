This folder contains files and code regarding the paper "Assessment of data quality in a Data Lakehouse through multi-granularity data profiling" accepted at ADBIS 2023.

Content:
- CSV files contains datasets used in the paper for the calculation of quality measures
- "dataset_generator" folder contains:
    - ds_generator.py: the main Python script for the generation of synthetic datasets
    - kg: the folder contains the Knowledge Graph as an RDF file
    - datasets: the output folder of the script where produced datasets will be stored
    - models: the KG.py class containing the code for the management of the Knowledge Graph
