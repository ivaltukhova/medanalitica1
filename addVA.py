import pandas as pd
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable


path_datasets = 'Datasets/'
xls_name = 'ВА.xlsx'
xls_file = path_datasets +xls_name
port = '11002'
bolt_url = 'bolt://localhost:' + port
user = "neo4j"
password = "GraphTest"

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def add_va(self, va):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_va, va)
            print(greeting)

    def add_va_rel_mo(self, va):
        #print(va)
        with self.driver.session() as session:
            result = session.write_transaction(self._create_va_rel_mo, va)
            print(result)

    @staticmethod
    def _create_va_rel_mo(tx, va):
        #q = "CREATE (" + mo + ")-[:ПОДВЕДОМСТВЕННОЕ]->(МинздравРК)"
        #print(q)
        query = ("MATCH (mo:MO { name: '" + row['МО'] + "'}), "
                       "(va:VA { name: '" + row['Подразделение'] + "', адрес: '" + va['Юридический адрес'] + "'}) " 
                 "MERGE (va)-[:ВХОДИТ]->(mo)")
        print(query)
        result = tx.run(query) # , mo=mo, mz='МинздравРК'
        print(result)
        print(result.single())
        return result.single()

    @staticmethod
    def _create_va(tx, va):
        query = ("MERGE (mo:VA { name: '" + va['Подразделение'] + "', midname: '" + va['ВА_Поселение'] + "', адрес: '" + va['Юридический адрес'] + "', модульное: '" + str(int(va['ВА Модульная'])) + "', поселение: '" + va['Поселение'] +"' })"
                 #" MATCH (mo:MO), (mz:IOrgan) WHERE mo.name = " + name + " AND mz.name = 'Минздрав РК'"
                 #" MERGE (mo)-[: ПОДВЕДОМСТВЕННОЕ]->(mz)"
                 )
        print(query)
        result = tx.run(query) # , name=name, fullname=fullname
        print(result)
        print(result.single())
        return result.single()


if __name__ == "__main__":
    df = pd.read_excel(xls_file)
    app = App(bolt_url, user, password)
    #print(df.columns)
    print(df.columns)
    print(df.head(5))
    for index, row in df.iterrows():
        print(row['МО'], row['Подразделение'])
        #app.add_va(row)
        app.add_va_rel_mo(row)