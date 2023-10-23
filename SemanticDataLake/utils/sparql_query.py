import rdflib

GRAPH = rdflib.Graph()
GRAPH.parse("knowladge_graph.ttl",format="turtle")

q=GRAPH.query("""SELECT ?x ?z ?y WHERE{?x owl:sameAs ?y. ?y property:rollup ?z}LIMIT 200""")

for row in q:
    print(f"{row.x},{row.z}")
