from rdflib import Bag, Graph, URIRef, Literal, BNode, Seq
from rdflib.namespace import OWL, RDF
from rdflib import Namespace

ns_project = Namespace("http://kdmg.dii.univpm.it/test/")
ns_kpionto = Namespace("http://w3id.org/kpionto/")

graph = None


def createDimension(dimension):
    uri = URIRef(ns_project + dimension)
    graph.add((uri, RDF.type, ns_kpionto.Dimension))
    print(dimension)


def createLevel(level, dimension, upper_level=None):
    uri_level = URIRef(ns_project + level)
    uri_dimension = URIRef(ns_project + dimension)
    graph.add((uri_level, RDF.type, ns_kpionto.Level))
    graph.add((uri_level, ns_kpionto.inDimension, uri_dimension))
    if upper_level:
        uri_upper_level = URIRef(ns_project + upper_level)
        graph.add((uri_level, ns_kpionto.rollup, uri_upper_level))
    print(">" + level)
    return

def generatePopulation(num_elements, lev, dimension, starting_counter, upper_member=None):
    population = []
    lev_name = "L" + str(lev) + "_D" + str(dimension)
    uri_level = URIRef(ns_project + lev_name)
    for elem in range(starting_counter, starting_counter + num_elements):
        elem_name = str(elem) + "_" + lev_name
        uri_elem = URIRef(ns_project + elem_name)
        graph.add((uri_elem, RDF.type, ns_kpionto.Member))
        graph.add((uri_elem, ns_kpionto.inLevel, uri_level))
        print(elem_name)
        population.append(elem_name)
        if upper_member != None:
            # link member to upper_member
            uri_upper_member = URIRef(ns_project + upper_member)
            graph.add((uri_elem, ns_kpionto.mRollup, uri_upper_member))
            print("\t(link ", elem_name, "->", upper_member, ")")

    return population


def _gen_level(population, actual_level, levels, actual_dimension, initial_population_size, g):
    actual_dimension_name = "D" + str(actual_dimension)
    # Base case
    if len(population) == 0:
        # generateLevel
        createLevel("L" + str(actual_level) + "_D" + str(actual_dimension), actual_dimension_name)
        population = generatePopulation(initial_population_size * g, actual_level, actual_dimension, 0)

    if actual_level < levels - 1:
        # Recursive case
        # generate sub_level and connect to the actual level
        sub_level_name = "L" + str(actual_level + 1) + "_D" + str(actual_dimension)
        actual_level_name = "L" + str(actual_level) + "_D" + str(actual_dimension)
        createLevel(sub_level_name, actual_dimension_name, actual_level_name)
        # for each item p in population, generate a sub_population
        sub_population = []
        for p in population:
            sub_population_counter = len(sub_population)
            ith_population = generatePopulation(initial_population_size * g, actual_level + 1, actual_dimension,
                                                sub_population_counter, p)
            # put all the sub-populations in an array sub_population
            sub_population.extend(ith_population)
        _gen_level(sub_population, actual_level + 1, levels, actual_dimension, initial_population_size, g)


def generateKG(dimensions, levels, initial_population, growing_factor):
    # for all dimensions
    for dim in range(0, dimensions):
        print("Dimensions: D" + str(dim))
        # create dimension in KG
        uri_dim = URIRef(ns_project + "D" + str(dim))
        graph.add((uri_dim, RDF.type, ns_kpionto.Dimension))
        population = []

        _gen_level(population, 0, levels, dim, initial_population, growing_factor)

    return


def main():
    global graph
    graph = Graph()
    graph.bind("kpi", ns_kpionto)
    graph.bind("ex", ns_project)
    # Generate the KG
    generateKG(5, 4, 10, 1)
    # Serialize the KG
    graph.serialize(destination="../kg/knowledge_graph_D5_L4_10.ttl", format="turtle")


if __name__ == "__main__":
    main()
