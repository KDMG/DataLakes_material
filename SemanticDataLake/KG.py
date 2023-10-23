from rdflib import Graph, URIRef
from rdflib import Namespace
from typing import List, Dict, Any


ns_project = Namespace("http://kdmg.dii.univpm.it/test/")
ns_kpionto = Namespace("http://w3id.org/kpionto/")


class KG:
    graph = None

    def __init__(self, graph_name):
        self.graph = Graph()
        self.graph.parse(graph_name, format="turtle")
        self.graph.bind("kpi", ns_kpionto)
        self.graph.bind("ex", ns_project)

    def get_dimensions(self) -> List[str]:
        """Returns the dimensions from the Knowledge Graph

        :returns: a list of dimensions names
        :rtype: list
        """
        result = self.graph.query(
            """SELECT ?x  WHERE {?x rdf:type kpi:Dimension}""")
        output = []
        for r in result:
            index = r[0].rfind('/')
            output.append(r[0][index + 1:])
        return output

    def get_levels(self, dimension: str = None) -> List[Any]:
        """Returns the levels for a given dimension from the Knowledge Graph

        :returns: a boolean representing the correct execution of the operation
        :rtype: list
        """
        if dimension:
            result = self.graph.query(
                """SELECT ?x  WHERE {?x rdf:type kpi:Level. ?x kpi:inDimension ex:%s}""" % dimension)
        else:
            result = self.graph.query(
                """SELECT ?x  WHERE {?x rdf:type kpi:Level}""")
        output = []
        for r in result:
            index = r[0].rfind('/')
            output.append(r[0][index + 1:])
        return output

    def get_members_from_level(self, level: Any, fragmentLevel: bool = False, fragmentOutput: bool = False) -> List[Any]:
        """Returns the list of members of a given level

        :param level: a level in the Knowledge Graph
        :type level: URIRef
        :param fragmentLevel: a boolean expressing whether the level parameter is a URIRef (False) or a string (True) (default is False).
        :type fragmentLevel: bool
        :param fragmentOutput: a boolean expressing whether the output is a list of strings (True) or URIRefs (True) (default is False).
        :type fragmentOutput: bool
        :returns: a boolean representing the correct execution of the operation
        :rtype: list
        """
        if fragmentLevel:
            level = ns_project + level
        result = self.graph.query(
            """SELECT ?x  WHERE {?x kpi:inLevel <%s>}""" % level)
        output = []
        for r in result:
            if fragmentOutput:
                index = r[0].rfind('/')
                output.append(r[0][index + 1:])
            else:
                output.append(r[0])
        return output

    def get_level_for_member(self, member: URIRef) -> List[URIRef]:
        """Returns the dimensions from the Knowledge Graph

        :param member: a member of a level in the Knowledge Graph
        :type member: URIRef
        :returns: a boolean representing the correct execution of the operation
        :rtype: list
        """
        result = self.graph.query(
            """SELECT ?x  WHERE {<%s> kpi:inLevel ?x}""" % member)
        output = []
        for r in result:
            output.append(r[0])
        return output

    def get_upper_level(self, level: URIRef) -> List[URIRef]:
        """Returns the upper level(s) for a given level

        :param level: the level
        :type level: URIRef
        :returns: a list of the URIs for upper levels
        :rtype: list
        """
        result = self.graph.query(
            """SELECT ?x  WHERE {<%s> kpi:rollup ?x}""" % level)
        output = []
        for r in result:
            output.append(r[0])
        return output
