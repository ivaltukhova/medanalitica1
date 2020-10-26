from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable


class Mednet:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def find_all(self):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_all)
            return result

    @staticmethod
    def _find_all_mo(tx):
        query = (
            '''MATCH (n:MO)
            RETURN n.`Наименование` AS name
            ORDER BY n.`Наименование`'''
        )
        result = tx.run(query)
        try:
            return {row["Наименование"]: row["Наименование"].title() for row in result}
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

