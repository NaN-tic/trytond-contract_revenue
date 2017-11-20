# The COPYRIGHT file at the top level of this repository contains
# the full copyright notices and license terms.
from sql.aggregate import Sum
from sql import Cast, Literal
from sql.functions import Substring, Position
from trytond.model import fields
from trytond.pool import Pool, PoolMeta
from trytond.pyson import Eval
from trytond.transaction import Transaction
from trytond.tools import reduce_ids
from decimal import Decimal

__all__ = ['Contract', 'ContractLine']


class Contract:
    __name__ = 'contract'
    __metaclass__ = PoolMeta

    revenue = fields.Function(fields.Numeric('Revenue',
            digits=(16, Eval('currency_digits', 2)),
            depends=['currency_digits']), 'get_total')
    cost = fields.Function(fields.Numeric('Cost',
            digits=(16, Eval('currency_digits', 2)),
            depends=['currency_digits']), 'get_total')

    @classmethod
    def get_total(cls, contracts, names):
        contract_ids = [x.id for x in contracts]
        values = {}
        for name in names:
            values[name] = dict().fromkeys(contract_ids, Decimal(0))

        for contract in contracts:
            for line in contract.lines:
                if 'cost' in names:
                    values['cost'][contract.id] += line.cost or Decimal(0)
                if 'revenue' in names:
                    values['revenue'][contract.id] += line.revenue
        return values


class ContractLine:
    __name__ = 'contract.line'
    __metaclass__ = PoolMeta

    revenue = fields.Function(fields.Numeric('Revenue',
            digits=(16, Eval('_parent_contract', {}).get('currency_digits', 2)),
            ), 'get_cost_and_revenue')
    cost = fields.Function(fields.Numeric('Cost',
            digits=(16, Eval('_parent_contract', {}).get('currency_digits', 2)),
            ), 'get_cost_and_revenue')

    @classmethod
    def get_cost_and_revenue(cls, lines, names):

        pool = Pool()
        Consumption = pool.get('contract.consumption')
        InvoiceLine = pool.get('account.invoice.line')

        consumption = Consumption.__table__()
        invoice_line = InvoiceLine.__table__()
        line_ids = [x.id for x in lines]
        currency_digits = lines and lines[0].contract.currency_digits or 2

        values = {}
        for name in names:
            values[name] = dict.fromkeys(line_ids, Decimal(0))

        cursor = Transaction().connection.cursor()
        table = cls.__table__()

        origin_id = Cast(Substring(invoice_line.origin,
            Position(',', invoice_line.origin) +
            Literal(1)), cls.id.sql_type().base)
        origin_model = Substring(invoice_line.origin,
            0, Position(',', invoice_line.origin))
        query = table.join(consumption, 'LEFT',
            condition=table.id == consumption.contract_line
            ).join(invoice_line, 'LEFT',
                condition=((consumption.id == origin_id) &
                    (origin_model == 'contract.consumption'))
            ).select(table.id, Sum(invoice_line.unit_price *
                    invoice_line.quantity),
                where=reduce_ids(table.id, line_ids),
                group_by=table.id
            )
        cursor.execute(*query)
        res = cursor.fetchall()

        for line, amount in res:
            if 'revenue' in names:
                values['revenue'][line] = Decimal(str(amount or 0.0)).quantize(
                    Decimal(str(10 ** - currency_digits )))
        return values
