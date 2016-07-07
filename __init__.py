# The COPYRIGHT file at the top level of this repository contains the full
# copyright notices and license terms.
from trytond.pool import Pool
from .contract import *

def register():
    Pool.register(
        Contract,
        ContractLine,
        module='contract_revenue', type_='model')
