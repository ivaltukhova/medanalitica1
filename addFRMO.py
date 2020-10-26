import pandas as pd
from neo4j import GraphDatabase
import time
# import numpy as np
# import logging
# from neo4j.exceptions import ServiceUnavailable

start_time = time.time()
path_datasets = 'Datasets/'
xls_name = 'ФРМО_СП_.xlsx'
xls_file = path_datasets + xls_name
port = '7687'
bolt_url = 'bolt://localhost:' + port
user = "neo4j"
password = "MedNet"


class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def add_frmo_row(self, row):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_frmo_row, row)
            print(greeting)

    # def add_mo(self, name, fullname):
    #    with self.driver.session() as session:
    #        greeting = session.write_transaction(self._create_mo, name, fullname)
    #        print(greeting)

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
                      " (mz:IOrgan { name: 'Минздрав РК'})" 
                 "CREATE (mo)-[:ПОДВЕДОМСТВЕННОЕ]->(mz)")
        print(query)
        result = tx.run(query, mo=mo, mz='МинздравРК')
        print(result)
        print(result.single())
        return result.single()

    @staticmethod
    def _create_frmo_row(tx, row):
        query_init = (" MERGE (pmsp:ВидПомощи { Наименование: 'ПМСП', "
                                              " Полное_наименование: 'Первичная медико-санитарная помощь'}) ")
        query = (" MERGE (subjectRF:СубъектРФ { Наименование: '" + row['Субъект РФ'] + "' }) ")
        # query_naspunkt = (" ")
        query_mun = ''
        if not row['НП'] == '':
            query_naspunkt = (" MERGE (nasPunkt:НасПункт { Наименование: '" + row['НП'] + "', "
                                " Префикс:'" + row['НП_префикс'] + "',"
                                " Численность_населения_Всего: '" + str(int(row['НП_численность_прожив.нас_Всего'])) + "', "
                                " Численность_населения_0_17_лет:'" + str(int(row['НП_численность_прожив.нас_0_17'])) + "', "
                                " Численность_населения_Взрослое:'" + str(int(row['НП_численность_прожив.нас_Взрослые'])) + "', "
                                " AOGUID: '" + row['НП_AOGUID'] + "'}) ")

            # Проверка вхождения населенного пункта в муниципальеное образование, если Нет - значит входит в субъект
            if row['НП_мунОбразование'] == '':
                query_mun = (" MERGE (munObraz:МунОбразование { Наименование: '" + row['НП'] + "' })"
                             " MERGE (munObraz)-[:ВХОДИТ]->(subjectRF) "
                             " MERGE (nasPunkt)-[:ВХОДИТ]->(munObraz)")
            else:
                query_mun = (" MERGE (munObraz:МунОбразование { Наименование: '" + row['НП_мунОбразование'] + "' }) "
                             " MERGE (munObraz)-[:ВХОДИТ]->(subjectRF) "
                             " MERGE (nasPunkt)-[:ВХОДИТ]->(munObraz) ")
        else:
            query_naspunkt = (" ")


        # Проверка ведомственной подчиненности МО
        if row['МО_Подчиненность'] == 'Органы исполнительной власти субъектов Российской Федерации, осуществляющие функции в области здравоохранения':
            # IO - исполнительный орган
            query_ved = (" MERGE (vedomstvo:Ведомство { Наименование: 'Минздрав РК', "
                                            " Полное_наименование: 'Министерство здравоохранения Республики Крым'}) ")
        elif row['МО_Подчиненность'] == 'Федеральное медико-биологическое агентство':
            query_ved = (" MERGE (vedomstvo:Ведомство { Наименование: 'ФМБА', "
                                            " Полное_наименование: 'Федеральное медико-биологическое агентство'}) ")
        else:
            query_ved = (" MERGE (vedomstvo:Ведомство { Наименование: '" + row['МО_Подчиненность'] + "', "
                                                       " Полное_наименование: '" + row['МО_Подчиненность'] + "'}) ")


        #
        query_mo = (" MERGE (mo:MO { Наименование: '" + row['МО_Наименование'] + "' , "
                                  " OID: '" + str(row['МО_OID']) + "', "
                                  " Вид_деятельности: '" + row['МО_Вид_деятельности'] + "', "
                                  " Тип_МО: '" + row['МО_Тип МО, оказывающей ПМСП'] + "', "
                                  " Проектная_мощность: '" + str(int(row['МО_Проектная_мощность'])) + "', "
                                  " Посещений_всего: '" + str(int(row['МО_посещений всего'])) + "', "
                                  " Посещений_по_ОМС: '" + str(int(row['МО_посещений по ОМС'])) + "'}) "
                    " MERGE (mo)-[:ПОДВЕДОМСТВЕННОЕ]->(vedomstvo) "
                    " MERGE (moAddress:Адрес { Адрес: '" +row['МО_Адрес МО']+ "', " 
                                             " AOGUID: '" + str(row['МО_AOGUID']) + "'}) "
                    " MERGE (moNasPunkt:НасПункт { Наименование: '" + str(row['МО_Префикс'] + " " + row['МО_Наименование наспункта']) + "' }) "
                    " MERGE (mo)-[:ЮР_АДРЕС]->(moAddress) "
                    " MERGE (moAddress)-[:НАХОДИТСЯ_В]->(moNasPunkt)"
        )

        # Структурное подразделение
        query_sp = (" MERGE (sp:СтруктурПодразделение { Наименование: '" + row['Наименование СП'] + "', "
                                                      " СП_OID: '" + row['СП OID'] + "' "
                                                      " } ) "
                    " MERGE (viewSP:ВидСтруктурПодразделения { Наименование: '" + row['Вид СП'] + "'} ) "
                    " MERGE (catSP:КатТипаСтруктурПодразделения { Наименование: '" + row['Категория типа СП'] + "'} )  "
                    " MERGE (sp)-[:ОТНОСИТСЯ_К_ВИДУ]->(viewSP) "
                    " MERGE (sp)-[:ОТНОСИТСЯ_К_КАТЕГОРИИ]->(catSP) "
                    " MERGE (sp)-[:ВХОДИТ_В_МО]->(mo) "
                    )

        # Здание СП
        query_build = ''
        if not row['Наименование здания'] == '':
            query_build = (" MERGE (building:Здание {Наименование: '" + row['Наименование здания'] + "',"
                                                   " Проектная_мощность: '" + str(row['Проектная мощность здания СП']) + "', "
                                                   " Плановая_мощность_амбул_подразделения: '" + str(row['Плановая мощность амбул.подразделения']) + "'}) "
                           " MERGE (adressBuild:Адрес {Адрес: '" + row['Адрес здания'] + "',"
                                                     " AOGUID: '" + str(row['AOGUID']) + "'} ) "
                           " MERGE (sp)-[:РАЗМЕЩАЕТСЯ_В]->(building) "
                           " MERGE (building)-[:НАХОДИТСЯ_ПО]->(adressBuild) ")

        if row['СП_Имеет прикреп.население'] == 'Да':
            q = (" MERGE (typeSP:ТипСтруктурПодразделения { Наименование: '" + row['Тип СП, оказывающего ПМСП'] + "'} ) "
                 " MERGE (sp)-[:ОТНОСИТСЯ_К_ТИПУ]->(typeSP) ")
            query_sp = query_sp + q

        # Если СП оказывает ПМСП - [:ОКАЗЫВАЕТ]->
        query_psmp = " "
        if row['СП_Оказывает ПМСП по ТУП'] == 'Да':
            query_psmp = (" MERGE (sp)-[:ОКАЗЫВАЕТ]->(pmsp) "
                          " MERGE (mo)-[:ОКАЗЫВАЕТ]->(pmsp) ")

            # Прикрепление населенного пункта к СП МО
            if not row['НП'] == '':
                q = (" MERGE (sp)-[:ОКАЗЫВАЕТ_ПМСП { Прикрепленное_население_Всего: '" + str(int(row['Общая численность прикреп.населения_СП_Всего'])) + "', "
                                               " Прикрепленное_население_0_17_лет:  '" + str(int(row['Общая численность прикреп.населения_СП_0_17'])) + "',"
                                               " Прикрепленное_население_Взрослые:  '" + str(int(row['Общая численность прикреп.населения_СП_Взрослые'])) + "'} ]->(nasPunkt) ")
                query_sp = query_sp + q
                # Доступность
                if row['Доступ_Нахождение_МО'] == 'Да':
                    q = (" MERGE (nasPunkt)-[:БЛИЖАЙШЕЕ_МО_СП {Расстояние: '" + str(int(row['Доступ_Расстояние_до_МО_СП'])) + "'} ]->(mo) ")
                else:
                    q = (" MERGE (nasPunkt)-[:БЛИЖАЙШЕЕ_МО_СП {Расстояние: '" + str(int(row['Доступ_Расстояние_до_МО_СП'])) + "'} ]->(sp) ")
                query_sp = query_sp + q


        #if (!str(row[15]) == '' ):
#        print(row['МО_Наименование мунОбразования'], type(row['МО_Наименование мунОбразования']))
#        if not row['МО_Наименование мунОбразования'] == '' :
#            munObr = ("MERGE (munObraz:МунОбразование { Наименование: '" + row['МО_Наименование мунОбразования'] + "'}) "
#                      "MERGE (nasPunct:НасПункт { Наименование:  '" + row['НП_Наименование наспункта'] + "', Префикс: '" + row['МО_Префикс'] + "'})"
#                      "MERGE (munObraz)-[:ВХОДИТ]->(subjectRF)")
        query = query_init + query + query_naspunkt + query_mun + query_ved + query_mo + query_sp + query_build + query_psmp
        print(query)
        result = tx.run(query)
        print(result)
        # print(result.single())
        return result.single()

def main():
    df = pd.read_excel(xls_file)
    df.fillna('', inplace=True)
    app = App(bolt_url, user, password)
    # print(df.columns)
    print(df.columns)
    # print(df_)
    df = df[1900:]
    s = 0
    for index, row in df.iterrows():
        print(s, row['Субъект РФ'])
        #        print(row[16], row[17])
        #        if (!str(row['МО_Наименование мунОбразования']) == '' ):

        app.add_frmo_row(row)
        s += 1
        # print(row['МО'], row['Наименование МО'])
        # app.add_mo(row['МО'], row['Наименование МО'])
        # app.add_mo_rel_mz(row['МО'])

if __name__ == "__main__":
    main()
    print("-- % seconds --" % (time.time() - start_time))