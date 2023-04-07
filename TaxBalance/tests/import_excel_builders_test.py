import unittest
from ..import_excel import UFNSTaxBalance


class TestExcelImport(unittest.TestCase):

    def test_UFNSTaxBalance_import(self):
        """На основании исходных файлов тестирует построение класса с контрольными суммами."""

        tax_balance = UFNSTaxBalance.add_from_excel("data/import_excel/Выписка операций по расчету с бюджетом Ангарский (2020).xlsx") \
            .add_from_excel("data/import_excel/Выписка операций по расчету с бюджетом Ангарский (2022).xlsx") \
            .add_from_excel("data/import_excel/Выписка операций по расчету с бюджетом Ангарский (2021).xlsx")
        tax_balance_length = len(tax_balance._tax_balance)
        self.assertEqual(tax_balance_length, 2493)