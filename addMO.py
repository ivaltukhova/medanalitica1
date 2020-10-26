import pandas as pd
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable


path_datasets = 'Datasets/'
filename = 'МО.csv'
xls_name = 'МО.xlsx'
file = path_datasets + filename
xls_file = path_datasets +xls_name
port = '11002'
bolt_url = 'bolt://localhost:' + port
user = "neo4j"
password = "GraphTest"

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def add_mo(self, name, fullname):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_mo, name, fullname)
            print(greeting)

    def add_mo_rel_mz(self, mo):
        print(mo)
        with self.driver.session() as session:
            result = session.write_transaction(self._create_mo_rel_mz, mo)
            print(result)

    @staticmethod
    def _create_mo_rel_mz(tx, mo):
        q = "CREATE (" + mo + ")-[:ПОДВЕДОМСТВЕННОЕ]->(МинздравРК)"
        print(q)
        query = ("MATCH (mo:MO { name: '" + mo + "'}), "
                       "(mz:IOrgan { name: 'Минздрав РК'})" 
                 "CREATE (mo)-[:ПОДВЕДОМСТВЕННОЕ]->(mz)")
        print(query)
        result = tx.run(query, mo=mo, mz='МинздравРК')
        print(result)
        print(result.single())
        return result.single()

    @staticmethod
    def _create_mo(tx, name, fullname):
        query = ("MERGE (mo:MO { name: '" + name + "', fullname: '" + fullname + "' })"
                 #" MATCH (mo:MO), (mz:IOrgan) WHERE mo.name = " + name + " AND mz.name = 'Минздрав РК'"
                 #" MERGE (mo)-[: ПОДВЕДОМСТВЕННОЕ]->(mz)"
                 )
        print(query)
        result = tx.run(query, name=name, fullname=fullname)
        print(result)
        print(result.single())
        return result.single()


if __name__ == "__main__":
    df = pd.read_csv(file)
    df_ = pd.read_excel(xls_file)
    app = App(bolt_url, user, password)
    print(df.columns)
    print(df_.columns)
    for index, row in df_.iterrows():
        print(row['МО'], row['Наименование МО'])
        #app.add_mo(row['МО'], row['Наименование МО'])
        app.add_mo_rel_mz(row['МО'])