import logging

import pandas as pd

from TaxBalance.import_excel import UFNSTaxBalance, SberPayments


class UFNSInsurancePaymentsValidated:
    """Подтвержденная переплата в развернутом виде с платежами"""

    # Перечень периодов по которым будет формироваться DF
    list_decisions = {
        '2021-4': {
            'dec': '2137',
            'payed_our': 1895901,
            'tax_report': 1296130,
        },
        '2021-3': {
            'dec': '8785',
            'payed_our': 2178329,
            'tax_report': 1447603,
        },
        '2021-2': {
            'dec': '7310',
            'payed_our': 2136134,
            'tax_report': 1541730,
        },
        '2021-1': {
            'dec': '4001',
            'tax_report': 1538689,
            'payed_our': 0,
        },
        '2020-4': {
            'dec': '1854',
            'tax_report': 1498729,
            'payed_our': 2519235.9,
        },
        '2020-3': {
            'dec': '1133',
            'tax_report': 1498729,
            'payed_our': 2304460,
        },
        '2020-2': {
            'document_numbers':
                ['12021', '012023', '12023', '12026', '12029', '12025', '12022',
                 '12033', '012031', '012022', '12034', '012024', '12030', '12024', '12020', '12032', '12027', '12035',
                 '12028'],
            'tax_report': 1595748,
            'payed_our': 0,
        },
    }

    # Перечень налогов по которым будет формироваться DF
    insurance_list = ['Страховые - Доп. Тариф', 'Страховые - ПФР', 'Страховые - ОМС',
                      'Страховые - Нетрудоспособность и Материнство']

    def __init__(self, ufns_balance: UFNSTaxBalance):
        self._df = ufns_balance.df

    def main(self):
        ins_balance = self._df
        for key, value in self.list_decisions.items():
            if 'document_numbers' in self.list_decisions[key].keys():
                ufns_tax_credit = 0
                ufns_penalty_credit = 0
                for doc in self.list_decisions[key]['document_numbers']:
                    mask = (ins_balance['document_number'] == doc) & (ins_balance['tax'].isin(self.insurance_list))
                    mask_tax = mask & (ins_balance['payment_type'] == 'Налог')
                    mask_other = mask & (ins_balance['payment_type'] != 'Налог')

                    ufns_tax_credit += ins_balance[mask_tax].credit.sum()
                    ufns_penalty_credit += ins_balance[mask_other].credit.sum()
            else:
                mask = (ins_balance['decision_number'] == value['dec']) & (ins_balance['tax'].isin(self.insurance_list))
                mask_tax = mask & (ins_balance['payment_type'] == 'Налог')
                mask_other = mask & (ins_balance['payment_type'] != 'Налог')
                ufns_tax_credit = ins_balance[mask_tax].credit.sum()
                ufns_penalty_credit = ins_balance[mask_other].credit.sum()

            our_ver = self.list_decisions[key]["payed_our"]

            self.list_decisions[key]['ufns_tax_credit'] = ufns_tax_credit
            self.list_decisions[key]['ufns_penalty_credit'] = ufns_penalty_credit
            self.list_decisions[key]['ufns_total_credit'] = ufns_penalty_credit + ufns_tax_credit
            self.list_decisions[key]['bank_ver'] = our_ver
            self.list_decisions[key]['delta'] = self.list_decisions[key]['ufns_total_credit'] - \
                                                self.list_decisions[key]['bank_ver']
            self.list_decisions[key]['overpayment'] = self.list_decisions[key]['ufns_total_credit'] - \
                                                      self.list_decisions[key]['tax_report']

            logging.debug(
                f'{key} - Разница(Н-МЫ): {ufns_penalty_credit + ufns_tax_credit - our_ver}  | Платеж: {ufns_penalty_credit + ufns_tax_credit} | Переплата: {ufns_penalty_credit + ufns_tax_credit - self.list_decisions[key]["tax_report"]}')

        result = pd.DataFrame.from_dict(self.list_decisions, orient='index')
        return result


class UFNSModelCurrentBalance:
    """Моделирует баланс по налогам опираясь исключительно на UFNSTaxBalance

        * Принимает за ноль момент списания (31.06.2021)
        * Убирает восстановление.
        * Убирает пени в момент восстановления

    """

    def __init__(self, ufns: UFNSTaxBalance):
        self.__ufns = ufns
        self.__tax_list = self.__ufns.df.tax.unique()  # Перечень всех налогов по которым будет выборка

    def __model(self):
        """Моделирует расчет по налогам"""

        tax_dict = {}
        for tax in self.__tax_list:
            # Считаем все начисления
            df = self.__ufns.df

            # Условия отбора
            mask = (df['document_number'] != '291')
            mask = mask & (df['tax'] == tax)
            mask = mask & (df['operation_date'] > pd.to_datetime('2021-06-30').date())
            mask = mask & (df.document_number != 'А69-3154/2022')  # Убираем дополнительные спорные документы

            # Заполняем словарь
            credit = df[mask].credit.sum()

            # Коррекция по 15%
            cor_mask = (df.operation_details == 'уменьшено (по декларации)')
            cor_mask = cor_mask & (df.document_registered_date > pd.to_datetime('2022-07-10').date())
            credit_correction = df[mask & cor_mask]['credit'].sum()

            debit_penni_291 = df[mask &
                                 (df.operation_details == 'начислены пени (по расчету)') &
                                 (df.operation_date == pd.to_datetime('2022-09-22').date())
                                 ].debit.sum() # Сколько дополнительно начислили пени при восстановлении
            debit_penni = df[mask & (df.operation_details == 'начислены пени (по расчету)')].debit.sum()
            debit = df[mask].debit.sum()

            tax_dict[tax] = {
                'balance': credit - debit + debit_penni_291,
                'credit': credit,
                'credit_correction': credit_correction,
                'debit': debit,
                'debit_penni': debit_penni,
                'debit_penni_291': debit_penni_291,
            }
            logging.debug(f'TAX: {tax} \nBalance: {credit - debit} \nC: {credit}, D:{debit} | D(Penni): {debit_penni}')

        return pd.DataFrame.from_dict(tax_dict, orient='index')

    def get_result(self) -> pd.DataFrame:
        return self.__model()


class UFNSView:
    """Основной класс-интерфейс для работы по отчетам УФНС и СберБанка.
    
    METHODS
    ---------------
    insurance_overpayment() -> pd.DataFrame: Переплаты по соц. страху до 2022 года.
    unidentified_insurance_payments() -> pd.DataFrame: Список неидентифицированных платежей, которые невозможно разнести к 
        какому-либо налогу. Возможна еще прибавка к платежам.
    correction_sum() -> pd.DataFrame: Объем принятой переплаты.
    model_balance() -> pd.DataFrame: Моделирует текущий баланс.
    current_balance() -> float: Текущее сальдо по налогам (в общих чертах).
    to_russian() - > pd.DataFrame: Переименовывает колонки на русский язык в соответствии с названием
    """

    COLUMNS_READABLE = ['operation_date', 'tax', 'payment_type', 'operation_details', 'credit', 'debit',
                        'decision_number',
                   'document_period', 'document_number', 'deadline', ]

    COLUMNS_RUSSIAN = {
        'operation_date': 'Дата Операции',
        'deadline': 'Срок уплаты',
        'operation_details': 'Операция',
        'document_registered_date': 'Документ: Дата пред. в НО',
        'document_type': 'Документ:Тип ',
        'document_number': 'Документ: Номер',
        'document_date': 'Документ: Дата',
        'document_period': 'Документ: Отч. период',
        'payment_type': 'Вид платежа',
        'debit': 'Дебет',
        'credit': 'Кредит',
        'accepted': 'Принято',
        'balance_by_type': 'Баланс по виду платежа',
        'balance_by_tax': 'Баланс по карточке',
        'payed_before_deadline': 'Досрочно погашена отсроченная задолженность',
        'tax': 'Налог'
    }

    def __init__(self):
        """"Инициализируем из Excel"""
        self.__sber = SberPayments() \
            .add_from_excel('data/Ангарский июль-декабрь 2020.xlsx') \
            .add_from_excel('data/СберБизнес. Выписка за 2021.01.01-2021.12.31 счёт 40702810465000000826.xlsx') \
            .add_from_excel('data/СберБизнес. Выписка за 2022.01.01-2022.12.31 счёт 40702810465000000826.xlsx')

        self.__ufns = UFNSTaxBalance().add_from_excel(
            "data/Выписка операций по расчету с бюджетом Ангарский (2020).xlsx", self.__sber) \
            .add_from_excel("data/Выписка операций по расчету с бюджетом Ангарский (2022).xlsx", self.__sber) \
            .add_from_excel("data/Выписка операций по расчету с бюджетом Ангарский (2021).xlsx", self.__sber)

    def insurance_overpayment(self) -> pd.DataFrame:
        """Подтвержденная переплата в развернутом виде с платежами.

        Не содержит неподтвержденных платежей (требуется подтвердить платежи из
        unidentified_insurance_payments() - Там еще > 400 тыс.руб.
        """
        return UFNSInsurancePaymentsValidated(self.__ufns).main()

    def unidentified_insurance_payments(self) -> pd.DataFrame:
        """Список неидентифицированных платежей по соц. страху, которые невозможно разнести к
        какому-либо налогу. Возможна еще прибавка к платежам.
        """
        insurance_list = ['Страховые - Доп. Тариф', 'Страховые - ПФР', 'Страховые - ОМС',
                          'Страховые - Нетрудоспособность и Материнство']

        balance = self.__ufns

        drop_operations = ['уплачено']

        mask = (balance.df['tax'].isin(insurance_list)) & (balance.df.operation_details.isin(drop_operations))

        # Выбираем во временном отрезке
        mask = mask & (balance.df['operation_date'] > pd.to_datetime('2020-12-01').date())

        columns = ['operation_date', 'tax', 'operation_details', 'credit', 'debit', 'decision_number',
                   'document_period', 'document_number']
        """Choosing without decision"""
        no_decision = balance.df[mask][columns]

        return no_decision[no_decision['decision_number'].isnull()]

    def model_balance(self):
        """Моделирует текущий баланс по налогам"""
        return UFNSModelCurrentBalance(self.__ufns).get_result()

    def operations_by_tax(self, tax: str) -> pd.DataFrame:
        """Не показывает 291, и некоторые документы, пока заточен под страховые
        Во вменяемом виде
        """
        df = self.df

        mask = (df['document_number'] != '291')
        mask = mask & (df['operation_date'] > pd.to_datetime('2021-06-30').date())
        mask = mask & (df['document_number'] != 'А69-3154/2022')  # Убираем дополнительные спорные документы
        mask = mask & (df['tax'] == 'Страховые - ОМС')

        cols = ['operation_date', 'credit', 'debit', 'deadline', 'document_number', 'document_type',
                'operation_details', 'document_period', 'tax']
        return df[mask][cols]

    def correction_sum(self) -> pd.DataFrame:
        """Возвращает по данным УФНС когда и куда был произведен зачет, в какие периоды.
        """
        # сколько зачлось по данным налоговой
        df = self.df.copy()

        mask = (df.operation_details == 'уменьшено (по декларации)')  # Только тут содержится коррекция
        mask = mask & (df.document_registered_date > pd.to_datetime('2022-07-10').date())
        df = df[mask]
        df['tax_period'] = df['deadline'] - pd.DateOffset(months=1) + pd.offsets.MonthEnd()

        df = df.groupby(by=['tax', 'tax_period'])['credit'].sum()
        return df.unstack()

    def current_balance(self, payments: float, accrual: float) -> float:
        """Возвращает расчетов сально с налоговой.

        Алгоритм
        Налоги[на последнее число операций.] - Берем из УФНС
        + банковские платежи - берем сразу из базы
        + Арест и Взыскание в Банк
        - начисления 4, 1 квартала.

        Пока в общих штрихах.

        :param accrual: Сколько было начислено с 2022-11-9.
        :param payments: Сколько было выплачено с 2022-11-9.
        """
        df = self.df
        last_day_operation = df.operation_date.iloc[-1]
        tax_balance_last_day = self.model_balance().balance.sum()
        return tax_balance_last_day + payments - accrual

    def to_russian(self, df):
        return df.rename(columns = self.COLUMNS_RUSSIAN)

    @property
    def df(self):
        return self.__ufns.df

    @property
    def sber(self):
        return self.__sber.df
