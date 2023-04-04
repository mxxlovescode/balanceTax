"""Модуль для построения оборотно сальдовой ведомости по налогам,

Необходимо
Возможности
----------------------
1) Определять сальдо на дату по версии налоговой
2) Сравнивать оплаты по периодам с нашей версией


------------------
1) Определить списанные налоги (их периоды, суммы)
2) Определить какие налоги были восстановлены
3) Куда было зачтены суммы наших переплат
"""
import logging
import re
from abc import ABC

import pandas as pd

"""Сопоставление КБК и Названия Налога"""
TAX_IDENTIFIERS = {

    '18210101012020000110': 'Прибыль',
    '18210102010010000110': 'НДФЛ',
    '18211603010010000140': 'Штрафы по ст.116, 119',
    '18210202010060010160': 'Страховые - ПФР',
    '18210202090070000160': 'Страховые - Нетрудоспособность и Материнство (до 1 января 2017)',
    '18210202090070010160': 'Страховые - Нетрудоспособность и Материнство',
    '18210202101080011160': 'Страховые - ОМС (до до 1 января 2017)',
    '18210202101080013160': 'Страховые - ОМС',
    '18210202132060010160': 'Страховые - Доп. Тариф',
    '18210202132060020160': 'Страховые - Доп. Тариф (не установлено)',
    '18211610129010000140': 'Штрафы (до 1 января 2020)',
    '18210301000010000110': 'НДС',
    '18210202010060000160': 'Страховые - ПФР (не установлено)',
    '18210101011010000110': 'Прибыль в ФЕД. Бюджет'
}


class ITaxBalance(ABC):
    """Базовый класс для Хранения налогового баланса"""
    pass


class UFNSTaxBalance(ITaxBalance):
    """Класс для управления налоговым балансом"""

    def __init__(self):
        self._tax_balance = pd.DataFrame()

    def get_payments(self, tax: str = None):
        """Возвращает все платежи (движение денежных средств) по банку и приставам
        (потом можно реализовать что будет абстрактный класс -
         платежи для унификации обработки

        """
        document_type = ['Платежный ордер', 'Инкассовое поручение', 'Платежное поручение']
        mask = self._tax_balance['document_type'].isin(document_type)

        if tax:
            mask_tax = (self._tax_balance['tax'] == tax)
            mask = (mask & mask_tax)

        return self._tax_balance[mask]

    def add_from_excel(self, path):
        """Добавляет в класс данные используя строитель UFNSTaxBalanceBuilder"""
        builder = UFNSTaxBalanceBuilder()
        builder.import_excel(path) \
            .clean_column_values() \
            .identify_columns() \
            .identify_taxes() \
            .validate_tax_identification() \
            .import_taxes()
        result = builder.get_result()

        self._tax_balance = pd.concat([self._tax_balance, result], axis=0, join='outer', ignore_index=True)
        logging.info(f'Загрузка файла завершена. В балансе содержится {len(self._tax_balance)} операций.')
        self._tax_balance = self._tax_balance.sort_values(by='operation_date')

        return self


class ExcelBuilderMixin:
    """Миксин с методами для импорта Excel Файлов

        * Обязательно необходимо при использовании вызвать init для инициализации внутренних переменных.

    """

    COLUMN_IDENTIFIERS = None

    def __init__(self):
        self._df = pd.DataFrame()  # Temp DataFrame with operations
        self._result = None  # Результат

    def import_excel(self, filepath: str = None):
        """Создает dataFrame из файла"""
        self._df = pd.read_excel(filepath)
        logging.debug(f"Импортируем Файл: {filepath}")
        return self

    def get_result(self):
        logging.info(f"Успешно импортировано {len(self._result)} строк.")
        return self._result

    def clean_column_values(self):
        """Безопасно чистит импортированную таблицу:
                    * Убирает специальные символы \\n из всех ячеек.
                    * Меняет переносы "- " на пустой символ. (Тут надо доработать, чтобы убирал только переносы текста).
                    * Убирает все полностью пустые строки.
        """
        self._df = self._df.dropna(axis=0, how='all').reset_index(drop=True)

        for col in self._df.columns:
            # If the column is not of numeric type
            if not pd.api.types.is_numeric_dtype(self._df[col]):
                # Replace '\n' with whitespace in all cells
                self._df[col] = self._df[col].apply(lambda x: str(x).replace('\n', ' '))
                self._df[col] = self._df[col].apply(lambda x: str(x).replace('- ', ''))
        return self

    def identify_columns(self):
        """Находит однозначно колонки входящие в список колонок COLUMN IDENTIFIERS и возвращает DF только с ними."""

        if self.COLUMN_IDENTIFIERS:
            cols_to_leave = {}
            for new_name, import_name in self.COLUMN_IDENTIFIERS.items():
                cols_with = self._df.columns[self._df.isin([import_name]).any()].tolist()
                if len(cols_with) > 1:
                    raise ValueError(
                        f"{import_name} - Невозможно однозначно определить колонку. \n Содержится в: {cols_with}")
                elif len(cols_with) == 0:
                    raise ValueError(f"{import_name} - Отсутствует такое значение во всех колонках. ")
                else:
                    logging.debug(f' Найдена: [{import_name}] - колонка: {cols_with[0]}')
                    cols_to_leave[cols_with[0]] = new_name

            self._df = self._df[list(cols_to_leave.keys())].rename(columns=cols_to_leave)
            return self
        else:
            raise NotImplementedError("Не определено COLUMN_IDENTIFIERS. Невозможно провести идентификацию.")


class UFNSTaxBalanceBuilder(ExcelBuilderMixin):
    """Строитель Оборотной Ведомости по файлам ЭКСЕЛЬ по операциям по лицевому счету налогоплательщика."""

    COLUMN_IDENTIFIERS = {
        'operation_date': 'Дата записи операции в карточку «Расчеты с бюджетом»',
        'deadline': 'Срок уплаты',
        'operation_details': 'Операция',
        'document_registered_date': 'дата пред. в НО',
        'document_type': 'вид',
        'document_number': 'номер',
        'document_date': 'дата',
        'document_period': 'отч. период',
        'payment_type': 'Вид платежа',
        'debit': 'дебет',
        'credit': 'кредит',
        'accepted': 'учтено',
        'balance_by_type': 'по виду платежа',
        'balance_by_tax': 'по карточке «Расчеты с бюджетом»',
        'payed_before_deadline': 'Досрочно погашена отсроченная задолженность',
    }

    class __UFNSLocalTaxBuilder:
        """Вспомогательный класс для функционирования UFNSTaxBalanceBuilder

        Строит баланс по конкретно-взятому налогу

        Разделяет Пени и Штрафы
        """

        def __init__(self, kbk, frame):
            self._tax_balance = None
            self._incoming_frame = frame
            self._kbk = kbk
            self._incoming_balance_tax = None
            self._incoming_balance_penalty = None

            self.logger = logging.getLogger()

        def clean_and_datetime_frame(self):
            """Оставляет записи конвертируемые в datetime и устанавливает тип datetime для полей с датами"""

            mask = pd.to_datetime(self._incoming_frame['operation_date'], errors='coerce', dayfirst=True).notnull()
            df = self._incoming_frame[mask].copy().reset_index(drop=True)

            self._tax_balance = df
            self.logger.debug(f"Загружено {len(df)} операций из {len(self._incoming_frame)} строк.")

            return self

        def set_types(self):
            """Устанавливает типы полей"""
            df = self._tax_balance

            self.logger.debug('Устанавливаем типы полей.')
            date_col = ['document_date', 'document_registered_date', 'deadline', 'operation_date']
            for col in date_col:
                df[col] = pd.to_datetime(df[col], dayfirst=True).dt.date

            num_col = ['payed_before_deadline', 'balance_by_tax',
                       'balance_by_type', 'accepted', 'credit', 'debit']
            df[num_col] = df[num_col].astype(float)
            df[num_col] = df[num_col].fillna(0)
            df['payment_type'] = df['payment_type'].astype('category')

            self._tax_balance = df

            return self

        def validate_balance_and_payments(self):
            """Проверяет, чтобы сходились обороты и изменения в балансе по мнению налоговой

            Проверяются отдельно Пеня и Налоги
            """
            tb = self._tax_balance
            checking_list = ['Налог', 'Пеня']

            for payment_type in checking_list:
                ch_df = tb[tb['payment_type'] == payment_type].sort_values(by='operation_date')
                if not ch_df.empty:
                    payment_delta = ch_df['debit'].sum() - ch_df['credit'].sum()
                    incoming_balance = ch_df.loc[ch_df.index[0], 'balance_by_type'] \
                                       + ch_df.loc[ch_df.index[0], 'debit'] \
                                       - ch_df.loc[ch_df.index[0], 'credit']
                    balance_delta = ch_df.loc[ch_df.index[-1], 'balance_by_type'] - incoming_balance
                else:
                    payment_delta = 0
                    balance_delta = 0

                if payment_delta + balance_delta > 10:
                    raise ImportError(f'[{TAX_IDENTIFIERS[self._kbk]}] - Ошибка при проверке контрольных сумм баланса. '
                                      f'Баланс по платежам: {payment_delta} | Баланс по Налоговой: {-balance_delta}')

                self.logger.debug(
                    f'Вид: {payment_type}. Баланс по платежам: {payment_delta} | Баланс по Налоговой: {-balance_delta}')
                self.logger.debug(f'[{TAX_IDENTIFIERS[self._kbk]}] - Проверка баланса и платежей прошла успешно.')
                return self

        def set_tax_and_kbk(self):
            self._tax_balance['tax'] = TAX_IDENTIFIERS[self._kbk]
            self._tax_balance['tax'] = self._tax_balance['tax'].astype('category')
            self._tax_balance['kbk'] = self._kbk
            self._tax_balance['kbk'] = self._tax_balance['kbk'].astype('category')

            return self

        def get_result(self):
            return self._tax_balance

    def __init__(self):
        super().__init__()
        self._tax_ranges = None

    def identify_taxes(self):
        """Проводит поиск налогов по КБК, определяет индексы записей по каждому КБК

            * Проверяет, чтобы КБК были уникальными иначе ValueError.
            * Заполняет словарь _tax_ranges соответствием КБК и интервала индексов.
            * Проверяет сколько строк разбито (должны быть разбиты все).
        """

        # Define variables for the data we want to extract
        logging.debug('Ищем Налоги.')

        start_phrases = ['КБК']
        key = ''
        value = []
        result_dict = {}
        pattern = r'\d{20}'  # Pattern of 20 digits KBK code

        # Iterate over dataframe rows
        for index, row in self._df.iterrows():
            # Check if current row's first column contains any of the start phrases
            if any(phrase in row[0] for phrase in start_phrases):
                # If current row's first column contains a new key, print previous key's value
                if 'КБК' in row[0]:
                    if key != '':
                        # Create dictionary entry with key and range of values

                        match = re.search(pattern, key)
                        if not match:
                            raise ValueError(f'КБК не найден в строке: {key}')
                        result_dict[match.group()] = list(range(value[0], index))
                    # Store new key and start index value
                    key = row[0]
                    value = [index]
                # Otherwise just add index to value list
                else:
                    value.append(index)

        match = re.search(pattern, key)
        if not match:
            raise ValueError(f'КБК не найден в строке: {key}')
        result_dict[match.group()] = list(range(value[0], len(self._df)))

        if len(set(result_dict.keys())) < len(result_dict.keys()):
            raise ImportError(
                f' Найдено: {len(result_dict.keys())} КБК.\n Уникальных: {len(set(result_dict.keys()))} КБК')

        # ЛОГИ
        message = ''
        for kbk in result_dict.keys():
            message += f'\n    - {TAX_IDENTIFIERS[kbk]} - [{kbk}]'
        logging.debug(f'Найдены следующие КБК: {message}')
        logging.debug(f'Найдено {len(result_dict.keys())} КБК.')

        self._tax_ranges = result_dict
        return self

    def validate_tax_identification(self):
        """Проверяет на целостность разноску налогов"""

        #  Сравнивает количество строк разнесенных и в файле, должно совпадать
        all_rows = []
        for key, value in self._tax_ranges.items():
            all_rows += value
        if len(self._df) - len(all_rows) > 10:
            raise ImportError(f'Слишком много строк выпало: распознано {len(all_rows)} строк из {len(self._df)}!')

        logging.debug(f'Проверка распознавания:  {len(all_rows)} строк из {len(self._df)} подтверждено.')
        return self

    def import_taxes(self):
        """Проходит по всем выявленным налогам и собирает в новую df"""
        for kbk, index_list in self._tax_ranges.items():
            tax_builder = self.__UFNSLocalTaxBuilder(kbk, self._df.loc[index_list])
            tax_builder.clean_and_datetime_frame() \
                .set_types() \
                .validate_balance_and_payments() \
                .set_tax_and_kbk()
            result = tax_builder.get_result()
            self._result = pd.concat([self._result, result], axis=0, join='outer', ignore_index=True)

        return self


class SberPaymentsExcelBuilder(ExcelBuilderMixin):
    """Строитель для импорта платежей из Excel-файлов платежей в адрес налоговой из Сбербанка."""

    COLUMN_IDENTIFIERS = {
        'operation_date': 'Дата проводки',
        'operation_sum': 'Сумма по дебету',
        'document_number': '№ документа',
        'reason': 'Назначение платежа',
    }

    def __init__(self):
        super().__init__()
        self.__validation_sum = None # Контрольная сумма оборотов для финальной валидации.

    def import_excel(self, filepath: str = None):
        super().import_excel(filepath)
        result = self._df.loc[self._df.eq('Итого оборотов').any(axis=1)].iloc[0]
        for value in result:
            # check if the value is numeric and greater than 0
            if isinstance(value, (int, float)) and value > 0:
                self.__validation_sum = value
                logging.debug(f'Найдены обороты: {value}')
        if not self.__validation_sum:
            logging.warning('Обороты не найдены!')

        return self

    def select_datetime_rows(self):
        """Оставляет строки содержащие дату в определенном столбца"""
        mask = pd.to_datetime(self._df['operation_date'], errors='coerce', dayfirst=True).notnull()
        self._result = self._df[mask].copy().reset_index(drop=True)
        logging.debug(f"Загружено {len(self._df)} операций из {len(self._df)} строк.")
        return self

    def select_validated_rows(self):
        """Отбрасываем там где сумма платежа не пустая"""
        dropped = self._result[self._result['operation_sum'].isna()]
        self._result = self._result[self._result['operation_sum'].notna()]
        if not dropped.empty:
            logging.debug(f'Дополнительно отброшено {dropped}')
        return self

    def set_types(self):
        """Определяем типы столбцов"""
        self._result['operation_date'] = pd.to_datetime(self._result['operation_date'], dayfirst=True).dt.date
        self._result['operation_sum'] = self._result['operation_sum'].fillna(0).astype('float64')
        self._result['reason'] = self._result['reason'].astype('category')
        logging.debug('Типы полей установлены.')
        return self

    def identify_decisions(self):
        """Определяет номера решений """

        def __find_decision(x):
            """Находит регулярное выражение с номером решения"""
            pattern = r'взыскании\s№\s+(\S+)\sот'
            correct_number = re.findall(pattern, x['reason'])
            if correct_number:
                x['decision_number'] = correct_number[0]
                return x
            else:
                logging.warning(f'Строка не распознана: \n    {x}')
                raise ValueError('Ошибка распознавания решения о взыскании!')

        self._result['decision_number'] = -1
        self._result = self._result.apply(lambda x: __find_decision(x), axis=1)
        logging.debug(f'Решения распознаны.')
        return self

    def validate_import(self):
        """Проводит необходимы проверки целостности файла сравнивая """
        check_sum = self._result['operation_sum'].sum()
        if  -1 < (check_sum - self.__validation_sum) < 1:
            logging.debug(f'Проверка контрольных сумм импорта. Успех. \n'
                          f'Сумма импорта: {self.__validation_sum}\n'
                          f'Количество операций: {len(self._result)}')
        else:
            raise ImportError(f'Контрольные суммы импорта не совпадают. \n'
                              f'Сумма документа: {self.__validation_sum} \nСумма импорта: {check_sum}')

        # Проверяем может ли к одному платежному документу относится два решения
        payment_docs = self._result.document_number.unique()
        for doc in payment_docs:
            unique_list = self._result[self._result['document_number'] == doc]['decision_number'].unique()
            decision_count = len(unique_list)
            if decision_count > 1:
                logging.warning(f'Найдена проблема: документ №{doc} содержит {decision_count} решений')
                raise ImportError(f'Один платежный документ погашает более одного решения о взыскании.')
        logging.debug(f'Проверка на соответствие платежных документов и решений о взыскании. Успех.')

        return self


class SberPayments:
    """Вектор платежей по расчетному счету в СБЕРБАНКЕ"""
    def __init__(self):
        self._operations = pd.DataFrame()

    def add_from_excel(self, path):
        """Импортирует файлы из ЭКСЕЛЬ"""
        operations = SberPaymentsExcelBuilder() \
            .import_excel(path) \
            .clean_column_values() \
            .identify_columns() \
            .select_datetime_rows() \
            .set_types() \
            .select_validated_rows() \
            .identify_decisions() \
            .validate_import() \
            .get_result()

        self._operations = pd.concat([self._operations, operations], axis=0, join='outer', ignore_index=True)\
            .sort_values(by='operation_date')
        logging.info(f'Загрузка файла завершена. В платежах содержится {len(self._operations)} операций.')

        return self


