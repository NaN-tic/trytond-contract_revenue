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
    currency_digits = fields.Function(fields.Integer('Currency Digits'),
        'on_change_with_currency_digits')


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


    @fields.depends('company')
    def on_change_with_currency_digits(self, name=None):
        if self.company:
            return self.company.currency.digits
        return 2

    @staticmethod
    def default_currency_digits():
        Company = Pool().get('company.company')
        company = Transaction().context.get('company')
        if company:
            company = Company(company)
            return company.currency.digits
        return 2


class ContractLine:
    __name__ = 'contract.line'
    __metaclass__ = PoolMeta

    revenue = fields.Function(fields.Numeric('Revenue',
            digits=(16, Eval('currency_digits', 2)),
            depends=['currency_digits']), 'get_cost_and_revenue')
    cost = fields.Function(fields.Numeric('Cost',
            digits=(16, Eval('currency_digits', 2)),
            depends=['currency_digits']), 'get_cost_and_revenue')
    currency_digits = fields.Function(fields.Integer('Currency Digits'),
        'on_change_with_currency_digits')

    @staticmethod
    def default_currency_digits():
        Company = Pool().get('company.company')
        company = Transaction().context.get('company')
        if company:
            company = Company(company)
            return company.currency.digits
        return 2

    @fields.depends('company')
    def on_change_with_currency_digits(self, name=None):
        if self.company:
            return self.company.currency.digits
        return 2

    @classmethod
    def get_cost_and_revenue(cls, lines, names):

        pool = Pool()
        Consumption = pool.get('contract.consumption')
        InvoiceLine = pool.get('account.invoice.line')

        consumption = Consumption.__table__()
        invoice_line = InvoiceLine.__table__()
        line_ids = [x.id for x in lines]
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
                values['revenue'][line] = Decimal(str(amount or 0.0))
        return values
