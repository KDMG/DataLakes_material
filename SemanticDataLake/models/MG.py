from rdflib import Graph, URIRef, Literal, BNode, RDF, XSD
from rdflib import Namespace
from datetime import date
from typing import List, Dict, Any

ns_project = Namespace("http://kdmg.dii.univpm.it/test/")
ns_kpionto = Namespace("http://w3id.org/kpionto/")
ns_datalake = Namespace("http://kdmg.dii.univpm.it/datalake/")
ns_dcterms = Namespace("http://purl.org/dc/terms/")
ns_void = Namespace("http://rdfs.org/ns/void#")


class MG:
    graph = None
    graph_name = None

    def __init__(self, graph_name: str):
        self.graph = Graph()
        self.graph_name = graph_name
        self.graph.parse(graph_name, format="turtle")
        self.graph.bind("kpi", ns_kpionto)
        self.graph.bind("dl", ns_datalake)
        self.graph.bind("void", ns_void)
        self.graph.bind("dcterms", ns_dcterms)
        self.selected_source = None
        self.selected_domain = None

    def get_all_paths(self) -> List[str]:
        """Extracts the filepaths from all the sources mounted in the Data Lake

        :returns: a list of strings representing the filepaths
        :rtype: list[str]
        """
        result = self.graph.query(
            """SELECT ?path WHERE {?x rdf:type dl:Source. ?x dl:location ?path}""")
        output = []
        for r in result:
            output.append(r[0])
        return output

    def get_sources(self) -> List[URIRef]:
        """Returns the URIs for all the sources in the Data Lake

        :returns: a list of the URIs (as URIRef) representing the identificators of the sources
        :rtype: list[URIRef]
        """
        result = self.graph.query(
            """SELECT ?x  WHERE {?x rdf:type dl:Source}""")
        output = []
        for r in result:
            output.append(r[0])
        return output

    def get_selected_source(self) -> URIRef:
        """Returns the source that has been selected, if any

        :returns: the URIRef of the source that has been selected, or None if no source has been selected
        :rtype: URIRef
        """
        if self.selected_source is not None:
            return self.selected_source
        else:
            return None

    def get_info_source(self, source: URIRef) -> Dict[str,Any]:
        """Returns all the metadata for a given source

        :param source: the URI of the data source
        :type source: URIRef
        :returns: a dictionary including uri, date, location, number of items and number of domains of the given source
        :rtype: dict
        """
        result = self.graph.query(
            """SELECT ?date ?location  ?items ?domains  WHERE {<%s> dcterms:date ?date; """ % source +
            """dl:location ?location; dl:items ?items; dl:domains ?domains.}""")

        output = dict()
        for r in result:
            output["uri"] = source
            output["date"] = r[0]
            output["location"] = r[1]
            output["items"] = r[2]
            output["domains"] = r[3]
        return output

    def get_mapped_domains(self, source: str) -> Dict[str, str]:
        """Returns a dictionary including, for the given source, domains with the corresponding mapped level in the Knowledge Graph

        :param source: the URI of the data source
        :type source: URIRef
        :returns: a dictionary including the URIRef of a domain and the corresponding URIRef of the level in the Knowledge Graph
        :rtype: dict
        """
        result = self.graph.query(
            """SELECT ?x ?n  WHERE {<%s> dl:contains ?x. ?x rdf:type dl:Domain. ?x dl:mapTo ?n}""" % source)
        output = dict()
        for r in result:
            output[r[0]] = r[1]
        return output

    def get_selected_domain(self) -> URIRef:
        """Returns the URI of the selected domain, if any

        :returns: the URI of the selected domain, or None if no domain has been selected
        :rtype: URIRef
        """
        if self.selected_domain is not None:
            return self.selected_domain
        else:
            return None

    def get_mapped_level(self, domain: str) -> URIRef:
        """Returns the level in the Knowledge Graph that is mapped to the given domain

        :param domain: the URI of a domain
        :type domain: URIRef
        :returns: the URI of the corresponding mapped level
        :rtype: URIRef
        """
        result = self.graph.query(
            """SELECT ?x ?n  WHERE {<%s> dl:mapTo ?x}""" % domain)
        output = None
        for r in result:
            output = r[0]
        return output

    def get_profiles(self, domain: URIRef) -> Dict[URIRef, int]:
        """Returns all the profile items for the given domain

        :param source: the URI of a domain
        :type source: URIRef
        :returns: a dictionary including the URI of a member in the Knowledge Graph and the corresponding number of occurrences
        :rtype: dict
        """
        output = dict()
        if domain is not None:
            result = self.graph.query(
                """SELECT ?m ?f  WHERE {<%s> dl:hasProfileElement ?b. """ % domain +
                """?b dl:toMember ?m. ?b dl:frequency ?f}""")
            for r in result:
                output[r[0]] = r[1].value
        return output

    def get_vect_profiles(self, domain: str, members: List[URIRef]) -> List[float]:
        """Returns a vectorial representation of the profile for a domain

        :param domain: the URI of the domain
        :type domain: URIRef
        :param members: the list of URI of members in the level which the domain is mapped to
        :type members: list[URIRef]
        :returns: a list of float representing the relative frequency of the i-th member of the level in the domain
        :rtype: list
        """
        output = []
        profiles = self.get_profiles(domain)
        profile_list = [str(key) for key in profiles]
        member_list = [str(item) for item in members]
        for m in sorted(member_list):
            if m in profile_list:
                output.append(profiles[URIRef(m)])
            else:
                output.append(0.0)
        return output

    def add_profile(self, source: str, domain: str, member: str, frequency: int) -> None:
        """Adds a profile item to the metadata graph for a domain in a source

        :param source: the URI of the source
        :type source: URIRef
        :param domain: the URI of the domain
        :type domain: URIRef
        :param member: the URI of the member of the profile item
        :type member: URIRef
        :param frequency: a int representing the number of occurrences of the member in the domain
        :type frequency: int
        """
        source_domain = source + "_" + domain
        profile_node = BNode()
        self.graph.add((URIRef(ns_project + source_domain), URIRef(ns_datalake + "hasProfileElement"), profile_node))
        self.graph.add((profile_node, ns_datalake.toMember, URIRef(member)))
        self.graph.add((profile_node, ns_datalake.frequency, Literal(frequency, datatype=XSD.integer)))

    def add_source(self, source: str, num_items: int, domains: int, filepath: str) ->None:
        """Adds a source to the metadata graph

        :param source: the URI of the source
        :type source: URIRef
        :param num_items: the number of items in the source
        :type num_items: int
        :param domains: the number of domains in the source
        :type domains: int
        :param filepath: the filepath of the source
        :type filepath: str
        """
        self.graph.add((URIRef(ns_project + source), RDF.type, URIRef(ns_datalake + "Source")))
        self.graph.add((URIRef(ns_project + source), RDF.type, URIRef(ns_void + "Dataset")))
        self.graph.add(
            (URIRef(ns_project + source), URIRef(ns_datalake + "location"), Literal(filepath, datatype=XSD.string)))
        self.graph.add(
            (URIRef(ns_project + source), URIRef(ns_dcterms + "date"), Literal(date.today(), datatype=XSD.date)))
        self.graph.add(
            (URIRef(ns_project + source), URIRef(ns_datalake + "items"), Literal(num_items, datatype=XSD.int)))
        self.graph.add(
            (URIRef(ns_project + source), URIRef(ns_datalake + "domains"), Literal(len(domains), datatype=XSD.int)))
        for d in domains:
            self.graph.add(
                (URIRef(ns_project + source + "_" + d), RDF.type, URIRef(ns_datalake + "Domain")))
            d = d.replace(" ", "_")
            self.graph.add(
                (URIRef(ns_project + source), URIRef(ns_datalake + "contains"), URIRef(ns_project + source + "_" + d)))

    def select_source(self, source: str) -> None:
        """Selects a source for performing actions on it

        :param source: a string representing the URI of the source or the numeric id of the source as shown in the list of ``sources''
        :type source: str
        """
        if source.isdigit():
            sources = self.get_sources()
            if int(source) < len(sources):
                self.selected_source = sources[int(source)]
                self.selected_domain = None
                print("Selected source: " + str(self.selected_source))
            else:
                print("The identificator does not match any source.")

        elif type(source) is str:
            if source in self.get_sources():
                self.selected_source = source
                self.selected_domain = None
                print("Selected source: " + str(self.selected_source))
            else:
                print("The identificator does not match any source.")
        else:
            print("No source is selected")

    def select_domain(self, domain: str) -> None:
        """Selects a domain for performing actions on it. A source must have been previously selected.

        :param domain: the URI of the domain or its numeric id as shown in the list through the ``describe'' command
        :type domain: str
        """
        if self.selected_source is not None:
            if domain.isdigit():
                doms = self.get_mapped_domains(self.selected_source)
                if int(domain) < len(doms):
                    k = list(doms.keys())[int(domain)]
                    self.selected_domain = k
                    print("Selected domain: " + str(self.selected_domain))
                else:
                    print("The identificator does not match any domain.")

            elif type(domain) is str and domain in self.get_mapped_domains(self.selected_source):
                selected_domain = domain
                print("Selected domain: " + str(self.selected_domain))
            else:
                print("The identificator does not match any domain.")
        else:
            print("No source is currently used. Please use a source.")

    def map(self, source: str, domain: str, level: str) ->None:
        """Adds the mapping between a domain in a source and a level in the Knowledge Graph

        :param source: the fragment for the URI of the source path
        :type source: str
        :param domain: the fragment for the URI of a domain
        :type domain: str
        :param level: the fragment of the URI of a level
        :type level: str
        """
        self.graph.add((URIRef(ns_project + source + "_" + domain), ns_datalake.mapTo, URIRef(ns_project + level)))

    def clear(self) -> bool:
        """Remove the selected source from the metadata graph.

        :returns: a boolean representing the correct execution of the operation
        :rtype: bool
        """
        if self.selected_source is not None:
            uri = self.selected_source
            try:
                # Get the columns nodes for the URI
                domains = self.graph.query(
                    """SELECT ?x WHERE {<%s> dl:contains ?x}""" % uri)
                for r in domains:
                    # Remove the profile nodes
                    node = r[0]
                    profile_nodes = self.graph.query("SELECT ?p WHERE {<%s> dl:hasProfileElement ?p}" % node)
                    for p in profile_nodes:
                        self.graph.remove((p[0], None, None))
                    self.graph.remove((r[0], None, None))
                self.graph.remove((uri, None, None))
                self.selected_domain = None
                self.selected_source = None
            except Exception as e:
                print(e.args)
                return False
            finally:
                return True
        else:
            print("No source is selected")
            return True

    def clear_all(self) -> bool:
        """Remove all sources from the metadata graph.

        :returns: a boolean representing the correct execution of the operation
        :rtype: bool
        """
        try:
            sources = self.get_sources()
            for s in sources:
                self.selected_source = s
                self.clear()
                print("\t- Source %s unmounted..." % s)
            self.serialize()
        except Exception:
            return False
        finally:
            return True

    def serialize(self):
        """Serializes the metadata graph in the storage. This operation needs to be done when some changes has been done on the metadata graph.
        """
        self.graph.serialize(destination=self.graph_name, format="turtle")
