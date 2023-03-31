from taxbalance import TaxBalanceDirector, UFNSTaxBalanceBuilder

director = TaxBalanceDirector(UFNSTaxBalanceBuilder, "TaxBalance/data/Выписка операций по расчету с бюджетом Ангарский (2020).xlsx")
director.build()
