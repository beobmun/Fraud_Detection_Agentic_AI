import pandas as pd
import ast
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD

class OntologyAdmin:
    def __init__(self, namespace_uri):
        self.g = Graph()
        self.OT = Namespace(namespace_uri)
        self.g.bind("ot", self.OT)

        print("OntologyAdmin initialized with namespace:", namespace_uri, flush=True)

    def init_schema(self):
        pass