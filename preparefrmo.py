import pandas as pd

df_frmo = pd.read_excel('Выгрузка из ФРМО.xlsx')


cols = ['Субъект РФ', 'МО_OID', 'МО_Наименование', 'МО_Вид_деятельности', 'МО_Тип МО, оказывающей ПМСП', 'МО_Кол-во обсл.населения_Всего', 'МО_Кол-во обсл.населения_0_17', 'МО_Кол-во обсл.населения_Взрослые', 'МО_Тип', 'МО_Подчиненность', 'МО_Проектная_мощность',
       'МО_посещений всего', 'МО_посещений по ОМС', 'МО_Наименование мунОбразования', 'МО_Наименование наспункта', 'МО_Префикс', 'МО_AOGUID', 'МО_Адрес МО', 'СП OID', 'Наименование СП', 'Вид СП',
       'Тип СП, оказывающего ПМСП', 'Категория типа СП', 'СП_Оказывает ПМСП по ТУП', 'СП_Имеет прикреп.население', 'Проектная мощность здания СП', 'Наименование здания', 'Наименование мун.образования', 'Наименование нас.пункта', 'Префикс нас.пункта', 'AOGUID',
       'Адрес здания', 'Плановая мощность амбул.подразделения', 'Кол-во нас.пунктов, обслуживаемых СП', 'Общая численность в обслуж. СП нас. пунктов на 2020', 'Прогнозная численность в обслуж. СП нас. пунктов на 2025', 'Общая численность прикреп.населения_СП_Всего', 'Общая численность прикреп.населения_СП_0_17', 'Общая численность прикреп.населения_СП_Взрослые', 'НП_субъект_в кот.распол', 'НП_мунОбразование',
       'НП', 'НП_префикс', 'НП_численность_прожив.нас_Всего', 'НП_численность_прожив.нас_0_17', 'НП_численность_прожив.нас_Взрослые', 'НП_Прогноз.численность_2025_0_17', 'НП_Прогноз.численность_2025_Взрослые', 'НП_прогноз.численность_Взрослые', 'НП_AOGUID', 'Доступ_Нахождение_МО',
       'Доступ_Расстояние_до_МО_СП', 'БлижМО_OID', 'БлижМО_Наименование_МО', 'БлижМО_Наименование_СПМО', 'БлижМО_Адрес_МО', 'БлижМО_Адрес_СП', 'БлижМО_Вид_деятельности_МО', 'БлижМО_Вид_СП_МО', 'ДоступПМСП_МО_находится_в_НП', 'ДоступПМСП_время_доезда_до_ближ_МО_СП',
       'ДоступПМСП_расстояние_доезда_до_ближ_МО_СП', 'БлижМОврПМСП_OID', 'БлижМОврПМСП_Наименование_МО', 'БлижМОврПМСП_Наименование_СПМО', 'БлижМОврПМСП_Адрес_МО', 'БлижМОврПМСП_Адрес_СПМО', 'БлижМОврПМСП_Вид_деятельности_МО', 'БлижМОврПМСП_Вид_СПМО', 'Строительство_Наименование_план.объекта', 'Строительство_Дата_получения_лицензии', 'Источник_финансирования']

def main():
    df_frmo = pd.read_excel('report_localities_subject91_2020-08-26-09-54-21.xlsx', skiprows=6, header=None, usecols="B:BU")
    df_frmo.columns=cols
    frmo_excel = pd.ExcelWriter('ФРМО_СП_.xlsx')
    df_frmo.to_excel(frmo_excel, 'ФРМО_СП')
    frmo_excel.close()

if __name__ == "__main__":
    main()