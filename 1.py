# import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# import xarray as xr
# import bokeh as bk

# from neo4jrestclient.client import GraphDatabase
# db = GraphDatabase('http://localhost:7474', username='neo4j', password='GraphTest')
from neo4j import GraphDatabase

class HelloWorldExample:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]


if __name__ == "__main__":
    greeter = HelloWorldExample("bolt://localhost:11002", "neo4j", "GraphTest")
    greeter.print_greeting("hello, world")
    greeter.close()
