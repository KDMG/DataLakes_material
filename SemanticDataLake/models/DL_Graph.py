from rdflib import Namespace, URIRef
from models.KG import KG
from models.MG import MG
from typing import List, Dict

ns_project = Namespace("http://kdmg.dii.univpm.it/test/")
ns_kpionto = Namespace("http://w3id.org/kpionto/")
ns_datalake = Namespace("http://kdmg.dii.univpm.it/datalake/")
ns_dcterms = Namespace("http://purl.org/dc/terms/")
ns_void = Namespace("http://rdfs.org/ns/void#")


class DL_Graph:

    def __init__(self, mg, kg):
        self.kg = KG(kg)
        self.mg = MG(mg)
        self.graph = self.kg.graph + self.mg.graph
        self.graph_name = "global graph"
        self.graph.bind("kpi", ns_kpionto)
        self.graph.bind("dl", ns_datalake)
        self.graph.bind("void", ns_void)
        self.graph.bind("dcterms", ns_dcterms)

    def query_one(self, query):
        result = self.graph.query(query)
        output = []
        for r in result:
            output.append(r[0])
        return output

    def query_two(self, query):
        result = self.graph.query(query)
        output = dict()
        for r in result:
            output[r[0]] = r[1]
        return output

    def get_rollup_level(self, level: URIRef, all_levels: bool) -> List[URIRef]:
        """Returns the upper level(s) for a domain

        :param level: the URIRef of a domain
        :type level: URIRef
        :param all_levels: a boolean specifying whether all upper levels should be returned or not
        :type all_levels: bool
        :returns: the list of URIRefs for the upper level(s)
        :rtype: list
        """
        if all_levels:
            result = self.graph.query(
                """SELECT ?x WHERE {<%s> kpi:rollup* ?x}""" % level)
        else:
            result = self.graph.query(
                """SELECT ?x WHERE {<%s> kpi:rollup ?x}""" % level)
        output = []
        for r in result:
            output.append(r[0])
        return output

    def get_profile_up(self, domain: URIRef) -> Dict[URIRef, int]:
        """Returns the upper level profile for a domain by aggregating the frequencies

        :param domain: the URIRef of a domain
        :type domain: URIRef
        :returns: a dictionary including a URIRef representing a member and the corresponding frequency
        :rtype: URIRef
        """
        result = self.graph.query(
            """SELECT ?mroll (SUM(?f) as ?sum)  WHERE {<%s> dl:hasProfileElement ?b. ?b dl:toMember ?m. """ % domain +
            """?m kpi:mRollup ?mroll. ?b dl:frequency ?f} GROUP BY ?mroll""")
        output = dict()
        for r in result:
            output[r[0]] = r[1].value
        return output

    def eval_completeness(self, domain: URIRef) -> float:
        """Returns the completeness level of the domain, i.e. considering the cardinality of the level to which the
        domain is mapped to, the completeness is computed as the ratio of level's members that are included in the
        domain as values

        :param domain: the URIRef of a domain
        :type domain: URIRef
        :returns: a float representing the completeness level
        :rtype: float
        """
        level = self.mg.get_mapped_level(domain)
        members = self.kg.get_members_from_level(level)
        return len(self.mg.get_profiles(domain)) / len(members)
