from pathlib import Path
from unittest import TestCase

from TaxBalance.import_excel import UFNSTaxBalance


class TestUFNSTaxBalance(TestCase):

    def test_UFNSTaxBalance_import(self):
        """На основании исходных файлов тестирует построение класса с контрольными суммами."""

        path_2020 = Path("data/import_excel/Выписка операций по расчету с бюджетом Ангарский (2020).xlsx")

        tax_balance = UFNSTaxBalance().add_from_excel(path_2020) \
            .add_from_excel("data/import_excel/Выписка операций по расчету с бюджетом Ангарский (2022).xlsx") \
            .add_from_excel("data/import_excel/Выписка операций по расчету с бюджетом Ангарский (2021).xlsx")
        tax_balance_length = len(tax_balance._tax_balance)
        self.assertEqual(tax_balance_length, 2493)
        self.assertEqual(tax_balance._tax_balance.debit.sum(), 60432721.61)
        self.assertEqual(tax_balance._tax_balance.credit.sum(), 52659436.77)
