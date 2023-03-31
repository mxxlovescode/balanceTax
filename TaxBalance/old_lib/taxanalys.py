"""Модуль содержит Классы для анализа данных по налогам


Порядок Работы с Модулем:
-------------------------
    1) Распознаем pdf - выписку операций по лицевому счету и загружаем через read_excel()
    2) Через FSNOperations.update() - дополняем базу другими годами (выписками)
    3) Поддерживаем актуальной Sqlite - вносим начисления и штрафы с которыми мы согласны
    4) Создаем модель нашей версии событий через COMOperations.model_company_version()


METHODS:
-------
    @class FNSOperations - класс для работы с операциями из Выписки по Лицевому счету налогоплательщика.
    @class COMOperations - Хранит операции и позицию нашей компании из базы и оперирует с ней (проводит эксперименты)
    read_excel - создает класс FNSOperations из Excel (распознанного pdf файла).



Доработка
---------

Обратить внимание на НДС и постараться его пересчитать




"""

import pandas as pd
from pandas import DataFrame
from pandas.tseries.offsets import MonthEnd

import sqlite3
import datetime
import re

"""КОНФИГУРАЦИЯ МОДУЛЯ"""

SQL_PATH = '/home/mt/PycharmProjects/taxanalys/data/tax_module'  # Расположение базы данных


class FNSOperations:
    """Хранит операции по лицевому счету налогоплательщика и позволяет с ними оперировать

    Attributes
    ----------
    operations : DataFrame с перечнем всех операций
    income_balance : Dictionary с самым ранним входящим сальдо ВИД {Налог : { Вид Платежа : Сумма}}

    Methods
    --------
     summary : DataFrame
            Считает итоги по наименованиям налогов

     __get_imported_balance : Dictionary
            Находит входящее сальдо по каждому налогу возвращает словарь

    add_balance : DataFrame
            Добавляет колонку Сальдо, опираясь на колонку ИЗМЕНЕНИЯ СУММЫ
    """

    def __init__(self, df_operations: DataFrame) -> None:
        self.operations = df_operations
        self.income_balance = self.__get_imported_balance()  # Сомнительно, когда будет функционал по датам, уберем

    def __get_imported_balance(self) -> dict:
        """Находит входящее сальдо по каждому налогу. Возвращает Словарь { Налог : {  Вид Платежа : Сумма }"""

        df = self.operations
        dic_balance_inc = {}

        for i, tax_category in enumerate(df['Налог Наименование'].unique()):

            dic_concrete_balance = {}

            mask = df['Налог Наименование'].isin([tax_category])
            concrete_tax = df[mask]
            concrete_tax = concrete_tax[concrete_tax['Операция'].str.contains('сальдо')]

            for payment_category in df['Вид Платежа'].unique():
                if payment_category in concrete_tax['Вид Платежа'].unique():
                    first_in_series = concrete_tax[
                        concrete_tax['Вид Платежа'] == payment_category
                        ].sort_values(by=['Дата Операции'])
                    first_in_series = first_in_series['Сальдо Расчетов По виду Платежа'].iloc[0]
                else:
                    first_in_series = 0

                dic_concrete_balance.update({
                    payment_category: first_in_series
                })

            dic_balance_inc.update(
                {tax_category: dic_concrete_balance}
            )

        return dic_balance_inc

    def get_balance(self, date_on: datetime, tax: str, sub_tax: str = 'Налог', period: str = 'D') -> float:
        """Возвращает сальдо на конец даты (дня, месяца, квартала), в который входит date_on

        Examples
        --------
            date_on = 27.02.2020 & period = D: Вернет сальдо по самой последней операции за этот день
                - Если на эту дату отсутствуют операции, вернет Сальдо на первую существующую в прошлое
                - Возвращает None если не в границе дат операций
        Parameters
        ----------
            date_on: На какую дату вывести сальдо
            sub_tax: Налог/Пени/Штраф - вид платежа по которому вывести сальдо
            period: (опционально) На какой период вывести сальдо [D, M, Q, Y - день, месяц, квартал, год]
                - (по умолчанию D - на конец дня)
            tax: Указание Налога По которому формируется Сальдо
        """

        mask = (
                self.operations['Налог Наименование'].isin([tax]) &
                self.operations['Вид Платежа'].isin([sub_tax])
        )
        balance_fns_df = self.operations[mask]
        date_range = self.operations['Дата Операции'].unique().tolist()
        date_on = pd.to_datetime(date_on, dayfirst=True)

        if "M" in period:
            date_on = pd.to_datetime(date_on) + MonthEnd(0)
        elif "Q" in period:
            date_on = pd.to_datetime(date_on) + pd.offsets.QuarterEnd()
        elif "Y" in period:
            date_on = pd.to_datetime(date_on) + pd.offsets.YearEnd()

        if (pd.to_datetime(max(date_range)) < date_on) or (date_on < pd.to_datetime(min(date_range))):
            balance = None
        else:
            period_df_found = balance_fns_df.loc[balance_fns_df['Дата Операции'] <= date_on]
            balance = period_df_found.iloc[-1]['Сальдо Расчетов По виду Платежа']

        return balance

    def summary(self, tax: str  = None, frequency: str = "Q", balance: bool = True) -> DataFrame:
        """Считает итоги по наименованиям налогов

        Parameters
        ----------
        tax : Наименование налога, либо перечень налогов, либо all - по всем налогам раздельно.
            По умолчанию: Сумма по всем налогам
        frequency : Частота по которой будут группироваться даты (месяц, неделя и.т.д)
            Опционально (по умолчанию: Квартал)
        balance : Выводить Сальдо.
            По умолчанию: Выводить

        Returns
        -------
        DataFrame
            Таблица с результатами по каждому виду налога
        """

        monthly_pivot_df = self.operations

        if tax and isinstance(tax, str) and 'all' not in tax:
            tax = [tax]
        elif tax and 'all' not in tax:
            monthly_pivot_df = monthly_pivot_df[monthly_pivot_df['Налог Наименование'].isin(tax)]

        monthly_pivot_df = monthly_pivot_df[
            ['Налог Наименование', 'Дата Операции',
             'Вид Платежа', 'Дебет Суммы',
             'Кредит Суммы', 'Операция']]
        monthly_pivot_df = monthly_pivot_df.reset_index(drop=True)
        monthly_pivot_df = monthly_pivot_df.set_index(
            ['Налог Наименование', 'Операция',
             'Вид Платежа', 'Дата Операции'
             ]).sort_index()

        level_values = monthly_pivot_df.index.get_level_values

        monthly_pivot_df = (monthly_pivot_df.groupby([level_values(i) for i in [0, 1, 2]]
                                                     + [pd.Grouper(freq=frequency, level=-1)]).sum(
            numeric_only=True)).reset_index()

        monthly_pivot_df = monthly_pivot_df.pivot_table(
            index=['Налог Наименование', 'Дата Операции', 'Вид Платежа'],
            values=['Дебет Суммы', 'Кредит Суммы'],
            aggfunc='sum')

        monthly_pivot_df['Изменение Суммы'] = monthly_pivot_df['Кредит Суммы'] - monthly_pivot_df['Дебет Суммы']

        if tax and 'all' not in tax:
            monthly_pivot_df = monthly_pivot_df.loc[tax]

        if balance:
            monthly_pivot_df = self.add_balance(monthly_pivot_df, self.income_balance)

        monthly_pivot_df.sort_index(inplace=True)

        if tax is None:
            monthly_pivot_df = monthly_pivot_df.groupby(level=[1, 2]).sum(numeric_only=True)

        return monthly_pivot_df

    @staticmethod
    def add_balance(df_balance: DataFrame, income_balance: dict) -> DataFrame:
        """Добавляет таблицу с Сальдо, опираясь на колонку ИЗМЕНЕНИЯ СУММЫ

        Attributes
        ----------
        df_balance : DataFrame имеющая колонку изменения суммы
        income_balance : Dictionary с самым ранним входящим сальдо на дату
        """

        df_balance['Сальдо'] = 0

        df_final = pd.DataFrame()
        for tax in df_balance.index.get_level_values(level=0).unique().tolist():
            changing_df = df_balance.loc[[tax]]
            changing_df.reset_index(inplace=True)

            balance_dic = income_balance[tax]
            for i in range(2):
                changing_df.at[changing_df.index[i], 'Сальдо'] = balance_dic[changing_df.iloc[i]['Вид Платежа']] + \
                                                                 changing_df.loc[i, "Изменение Суммы"]

            for i in range(len(changing_df.index)):
                if 0 <= i < (len(changing_df.index) - 3):
                    changing_df.at[i + 3, "Сальдо"] = changing_df.loc[i, "Сальдо"] + changing_df.loc[
                        i + 3, "Изменение Суммы"]

            changing_df = changing_df.set_index(['Налог Наименование', 'Дата Операции', 'Вид Платежа']).sort_index()
            df_final = pd.concat([df_final, changing_df], join='outer')

        return df_final

    def update(self, new_tx) -> None:
        """Добавляет операции из другого класса FNSOperations """
        self.operations = pd.concat([self.operations, new_tx.operations], join='outer')
        self.operations.drop_duplicates(inplace=True, keep='last')


class COMOperations:
    """Хранит операции и позицию нашей компании из базы и оперирует с ней (проводит эксперименты)

    Attributes
    ----------
        operations : DataFrame с перечнем всех операций
        acurral_duties : Смоделированное налоги по дате появление на лицевом счете

     Methods
    --------
        read_company_acurral: DataFrame

        model_acurral_duties: DataFrame

        model_company_version
    """

    def __init__(self, init_operations) -> None:

        self.operations = init_operations
        self.acurral_duties = self.model_acurral_duties()

    @staticmethod
    def read_company_acurral(db_path: str = SQL_PATH):
        """Возвращает Таблицу Официальной Нашей Позиции по начислению из Sqlite3"""

        con = sqlite3.connect(db_path)

        taxes_df = pd.read_sql('SELECT * from reports_official', con, parse_dates='date_quarter', index_col='index')
        taxes_df = taxes_df.rename(columns={'date_quarter': 'Дата Операции',
                                            'name_tax': 'Налог Наименование',
                                            'name_sub_tax': 'Вид Платежа',
                                            'accural': 'Дебет Суммы'}).set_index(
            ['Дата Операции', 'Налог Наименование', 'Вид Платежа']).sort_index()

        return COMOperations(taxes_df)

    def model_acurral_duties(self) -> DataFrame:
        """Расставляет когда начисления появляются на лицевом счете и возникает обязанность платить налог"""

        taxes_df = self.operations
        """Приводим начисления по НДФЛ за 2021 год в один месяц"""
        ndfl_2021 = taxes_df.loc[(slice(None), 'НДФЛ'), :].sort_index().loc['20210330':'20210930'].sum()
        taxes_df.sort_index(inplace=True)
        taxes_df.loc[(slice('2021-03-31', '2021-09-30'), 'НДФЛ'), :] = 0
        taxes_df.loc[('2021-12-31', 'НДФЛ', 'Налог'), :] = taxes_df.loc[('2021-12-31', 'НДФЛ', 'Налог'), :] + ndfl_2021

        taxes_df = taxes_df.sort_index().loc['20210331':]  # Только до этой даты есть статистика

        """В следующем блоке сдвигаем на 3 месяца все даты чтобы дата совпадала с датой операций по лицевому счету"""

        taxes_df.reset_index(inplace=True)
        taxes_df['Дата Операции'] = taxes_df['Дата Операции'].apply(
            lambda x: x + pd.DateOffset(months=3)) + pd.offsets.MonthEnd(0)
        taxes_df = taxes_df.set_index(['Дата Операции', 'Налог Наименование', 'Вид Платежа']).sort_index()

        taxes_df = taxes_df.loc['2021-09-30':].sort_index()

        return taxes_df

    def model_company_version(self, tox: FNSOperations) -> DataFrame:
        """Моделирует задолженность на конец третьего квартала без Пени, но с налогом на Прибыль с учетом Пеней)

        Parameters
        ----------
        tox: Экземпляр FNSOperations, содержащий операции по лицевому счету по данным УФНС
        company_acurral: Таблица вида FNSOperation.summary содержащее ДЕБЕТ СУММЫ - начисления компании

        Вводные Данные:
            - Сальдо на начало третьего квартала 2021 равно нулю по всем видам платежей и налогов (в связи со списанием) +
            - Второй Квартал 2022 не начисляется по страховым (стало быть начисление которе в третьем квартале стоит убрать)
            - Убираем из 3 квартала 2022 уменьшение по декларациям из оплат (Кредит Суммы) +
            - Начисление НДФЛ за весь 2021 год отражено в 1 квартале 2022 +

        Поля Вывода:
            - [Кредит Суммы (УФНС)] - Берем поступления денежных средств по данным УФНС
            - [Дебет Суммы (Модель)] - Берем начисление налогов по нашим данным (с учетом корректировки)
        """

        company_acurral = self.acurral_duties

        """Убираем из Кредита Уменьшение по Декларации (проводки уплат)

        2022-07-15 - Корректировка первого квартала 2022
        2022-07-20 - Корректировка 2020-2021
        
        Оставили только по платежным поручениям приходы (все остальные отражают внутренние перетоки налоговой которые 
        нам не интересны. 
        """

        df_operations = tox.operations.set_index(['Налог Наименование', 'Дата Операции', 'Вид Платежа']).sort_index()
        df_operations = df_operations.swaplevel(0, 1).sort_index()
        df_operations = df_operations[
            df_operations['Вид Документа'].str.contains('Платежно', case=False)]  # Убираем уменьшения по декларациям и так далее

        tox_no_decrease = FNSOperations(df_operations.reset_index())

        df_operations = tox_no_decrease.summary(tax='all', frequency='Q', balance=False).swaplevel(0, 1).sort_index()

        what_null = [  # Что не начисляется
            'Страховые - Доп. Тариф',
            'Страховые - Материнство',
            'Страховые - ОМС',
            'Страховые - ПФР'
        ]

        import_taxes_df = company_acurral.swaplevel(0, 1).sort_index()
        import_taxes_df.loc[(what_null, '2022-09-30', 'Налог'), :] = 0

        import_taxes_df = import_taxes_df.swaplevel(0, 1)
        """Собираем воедино Кредит УФНС и Дебет (Модель)"""
        model_df = import_taxes_df.merge(df_operations[['Кредит Суммы']], left_index=True, right_index=True, how='left')

        """Делаем вывод Таблицы"""
        model_df.fillna(0, inplace=True)
        model_df = model_df[['Дебет Суммы', 'Кредит Суммы']]
        model_df['Дебет Суммы'] = model_df['Дебет Суммы'].astype('float')
        model_df = model_df.groupby(level=[0, 2]).sum()
        model_df['Изменение Суммы'] = model_df['Кредит Суммы'] - model_df['Дебет Суммы']

        """ Считаем ПЕНИ и Сальдо расчетным способом. Квартальное сальдо умножаем на пени и на 3. 
        Очень грубый расчет, ставка рефинансирования взята как 8,5, считается только по кварталу
        """

        model_df['Сальдо'] = 0

        k_peni = 91 * 0.085 / 150  # Размер (процент) начисляемой в квартал Пени -

        for i in range(len(model_df.index.to_list())):

            if model_df.index[i][1] == 'Пеня':
                model_df.at[model_df.index[i], 'Дебет Суммы'] = model_df.iloc[i]['Дебет Суммы'] \
                                                                - k_peni * model_df.iloc[i - 1]['Сальдо']
                model_df.at[model_df.index[i], 'Изменение Суммы'] = model_df.iloc[i]['Кредит Суммы'] - \
                                                                    model_df.iloc[i]['Дебет Суммы']

            model_df.at[model_df.index[i], 'Сальдо'] = model_df.iloc[i]['Изменение Суммы'] + model_df.iloc[i - 3][
                'Сальдо']

        return model_df


class FNSDocs:
    """Оперирует с официальными документами из налоговой"""

    def __init__(self, ver: int = 0):
        """Если 0, то включает инициализацию из DataScience """




def read_excel(file_path: str, ver: int = 1) -> FNSOperations:
    """Импортирует Операции по лицевому счету налогоплательщика из xlsx, возвращает класс FNSOperations

    Parameters
    ----------
    @file_path : Путь к импортируемому файлу (переведенному из pdf в xlsx на сайте www.pdf2go.com/ru/pdf-to-excel)
    @ver: Встречаются уже две версии эксель импорта 1 или 2 (так загрузился импорт 15 ноября)

    Returns
    -------
    FNSOperations : Готовый к работе

    """

    def rename_columns(df_to_rename: DataFrame) -> DataFrame:
        """Переименовывает Колонки"""

        df_to_rename.rename(columns={
            df_to_rename.columns[0]: 'Дата Операции',  # Дата записи в карточку "Расчета с Бюджетом" - техническая дата
            df_to_rename.columns[1]: 'Срок Уплаты',  # ВЫЯСНИТЬ ЧТО ЭТО
            df_to_rename.columns[2]: 'Операция',  # Категория операции, вид операции
            df_to_rename.columns[3]: 'Дата Пред. в НО Документа',  # ? Дата когда документ поступил в НО
            df_to_rename.columns[4]: 'Вид Документа',  # Категория документа на основании которого проведена операция,
            df_to_rename.columns[5]: 'Номер Документа',  # Номер документа на основании которого проведена операция
            df_to_rename.columns[6]: 'Дата Документа',  # ? Дата издания документа
            df_to_rename.columns[7]: 'Отчетный Период Документа',  # В случае с декларацией отчетный период
            df_to_rename.columns[8]: 'Вид Платежа',  # Вид платежа
            df_to_rename.columns[9]: 'Дебет Суммы',  # Задолженность (увеличение долга)
            df_to_rename.columns[10]: 'Кредит Суммы',  # Платеж задолженности (оплата долга)
            df_to_rename.columns[11]: 'Сальдо Расчетов По виду Платежа',  # ?
            df_to_rename.columns[12]: 'Сальдо По карточке Расчетов с Бюджетом',  # ?
        }, inplace=True)

        df_to_rename = df_to_rename[df_to_rename['Дата Операции'].notnull()].reset_index(drop=True)

        return df_to_rename

    def set_tax_category(df_no_category: DataFrame) -> DataFrame:
        """Добавляет название налога в новый столбец "Налог Наименование"
        Parameters
        ----------
        @df_no_category : DataFrame

        Returns
        -------
        DataFrame

        """
        mask = df_no_category[
            df_no_category['Дата Операции'].apply(lambda x: not isinstance(x, datetime.datetime)) &
            df_no_category['Дата Операции'].str.contains('Наименование Налога', regex=True, case=False)]

        tax_labels__to_rename = {
            'Наименование налога: Налог на добавленную стоимость на товары (работы, услуги), реализуемые на '
            'территории Российской Федерации': 'НДС',

            'Наименование налога: Страховые взносы на обязательное пенсионное страхование в Российской Федерации, '
            'зачисляемые в Пенсионный фонд Российской': 'Страховые - ПФР',

            'Наименование налога: Налог на прибыль организаций (за исключением консолидированных групп '
            'налогоплательщиков), зачисляемый в федеральный бюдж': 'Налог Прибыль Фед. Бюджет',

            'Наименование налога: Налог на прибыль организаций (за исключением консолидированных групп '
            'налогоплательщиков), зачисляемый в бюджеты субъекто': 'Налог Прибыль Рег. Бюджет',

            'Наименование налога: Налог на доходы физических лиц с доходов, источником которых является налоговый '
            'агент, за исключением доходов, в отношен': 'НДФЛ',

            'Наименование налога: Денежные взыскания (штрафы) за нарушение законодательства о налогах и сборах, '
            'предусмотренные статьями 116, статьей 119.': 'Штрафы (не ясно)',

            'Наименование налога: Страховые взносы на обязательное социальное страхование на случай временной '
            'нетрудоспособности и в связи с материнством': 'Страховые - Материнство',

            'Наименование налога: Страховые взносы на обязательное медицинское страхование работающего населения, '
            'зачисляемые в бюджет Федерального фонда': 'Страховые - ОМС',

            'Наименование налога: Страховые взносы по дополнительному тарифу за застрахованных лиц, занятых на '
            'соответствующих видах работ, указанных в пу': 'Страховые - Доп. Тариф',

            'Наименование налога: Доходы от денежных взысканий (штрафов), поступающие в счет погашения '
            'задолженности, образовавшейся до 1 января 2020 года': 'Доходы от Штрафов ',
        }

        df_no_category['Налог Наименование'] = ''

        pre_index = 0
        for op_index in mask['Дата Операции'].index:

            if df_no_category['Дата Операции'].iloc[pre_index] in tax_labels__to_rename:
                label_tax = tax_labels__to_rename[df_no_category['Дата Операции'].iloc[pre_index]]
            else:
                label_tax = 'NO CATEGORY'

            df_no_category.loc[
                df_no_category.index[pre_index:op_index],
                'Налог Наименование'] = label_tax
            pre_index = op_index

        label_tax = tax_labels__to_rename[df_no_category['Дата Операции'].iloc[pre_index]]
        df_no_category.loc[
            df_no_category.index[pre_index:],
            'Налог Наименование'] = label_tax

        df_no_category = df_no_category[~df_no_category['Налог Наименование'].str.contains('NO CATEGORY')]

        return df_no_category

    def set_column_types(df_types: DataFrame) -> DataFrame:
        """Возвращает DataFrame с определенным типом колонок"""

        df_types['Дата Операции'] = pd.to_datetime(df_types['Дата Операции'], dayfirst=True)

        df_types['Срок Уплаты'] = pd.to_datetime(df_types['Срок Уплаты'], dayfirst=True)

        df_types['Операция'] = df_types['Операция'].apply(lambda x: re.sub(r'- |\n|-', '', x))
        df_types['Операция'] = df_types['Операция'].astype('category')

        df_types['Дата Пред. в НО Документа'] = pd.to_datetime(df_types['Дата Пред. в НО Документа'], dayfirst=True)

        df_types['Вид Документа'] = df_types['Вид Документа'].fillna('Не определено')
        df_types['Вид Документа'] = df_types['Вид Документа'].apply(lambda x: re.sub(r'- |\n|-', '', x))
        df_types['Вид Документа'] = df_types['Вид Документа'].astype('category')

        df_types['Номер Документа'] = df_types['Номер Документа'].astype('string')

        df_types['Дата Документа'] = pd.to_datetime(df_types['Дата Документа'], dayfirst=True)

        df_types['Отчетный Период Документа'] = df_types['Отчетный Период Документа'].astype('string')

        df_types['Вид Платежа'] = df_types['Вид Платежа'].astype('category')

        df_types['Дебет Суммы'] = df_types['Дебет Суммы'].astype('float')

        df_types['Сальдо Расчетов По виду Платежа'] = df_types['Сальдо Расчетов По виду Платежа'].astype('float')

        df_types['СальдоПо карточке Расчетов с Бюджетом'] = df_types['Сальдо По карточке Расчетов с Бюджетом'].astype(
            'float')

        df_types['Кредит Суммы'] = df_types['Кредит Суммы'].astype('float')

        df_types['Налог Наименование'] = df_types['Налог Наименование'].astype('category')

        return df_types

    def check_date_time(cell_value) -> bool:
        """Проверяет, содержится ли дата в ячейке по определенному типу"""
        try:
            datetime.datetime.strptime(str(cell_value), '%d.%m.%Y')
            return True

        except Exception:
            return False

    df_to_import = pd.read_excel(file_path)

    if ver == 1:
        df_to_import.drop(columns=[df_to_import.columns[5], 'Unnamed: 12', 'Unnamed: 15'], inplace=True)
    elif ver == 2:
        df_to_import.dropna(axis=1, how='all', inplace=True)
        df_to_import.drop(columns=['Unnamed: 1', 'Unnamed: 4', 'Unnamed: 5', 'Unnamed: 6',
                                   'Unnamed: 8', 'Unnamed: 11', 'Unnamed: 14', 'Unnamed: 15', 'Unnamed: 20',
                                   'Unnamed: 30',
                                   'Unnamed: 31', 'Код по КНД 1166107', 'Unnamed: 36', 'Unnamed: 37'], inplace=True)
    df_to_import = rename_columns(df_to_import)

    df_to_import = set_tax_category(df_to_import)

    # Оставляем только те строки, где есть дата операции
    if ver == 1:
        df_to_import = df_to_import[df_to_import['Дата Операции'].apply(lambda x: isinstance(x, datetime.datetime))]
    else:
        df_to_import = df_to_import[df_to_import['Дата Операции'].apply(lambda x: check_date_time(x))]

    df_to_import = set_column_types(df_to_import)

    df_to_import = df_to_import.reset_index(drop=True)

    return FNSOperations(df_to_import)
