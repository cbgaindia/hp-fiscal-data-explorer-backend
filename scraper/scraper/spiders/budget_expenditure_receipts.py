'''
Budget expenditure and receitps spiders
'''
# -*- coding: utf-8 -*-
from scraper.spiders import budget_base


class BudgetExpendituresSpider(budget_base.ExpenditureBaseSpider):
    '''
    BudgetExpendituresSpider
    '''
    name = 'budget_expenditures'

    start_urls = ['https://himkosh.hp.nic.in/treasuryportal/eKosh/ekoshhodAllocationquery.asp']

    # dataset is collected from here.
    query_url = 'https://himkosh.hp.nic.in/treasuryportal/eKosh/eKoshHODAllocationPopUp.asp?{}'

    query_index = 9

    unit = '.00001'  # amount unit, .001: Thousands, .00001: Lakhs, 1: Rupees

class BudgetReceiptsSpider(budget_base.ReceiptBaseSpider):
    '''
    BudgetReceiptsSpider
    '''
    name = 'budget_receipts'

    # dataset is collected from here.
    query_url = 'https://himkosh.nic.in/eHPOLTIS/PublicReports/Receipt.asmx/GetReceiptMajorDetail'

    unit = '1'  # index of the unit of amount, 0: Rupees, 1: Thousand, 2: Lakh
