# -*- coding: utf-8 -*-
'''
Spiders for Treasury Expenditure and Receipts
'''

from scraper.spiders import treasury_base


class TreasuryExpendituresSpider(treasury_base.TreasuryBaseSpider):
    '''
    Spider for Treasury Expenditures.
    '''
    name = 'treasury_expenditures'

    start_urls = ['https://himkosh.hp.nic.in/treasuryportal/eKosh/ekoshddoquery.asp']

    # dataset is collected from here.
    query_url = 'https://himkosh.hp.nic.in/treasuryportal/eKosh/eKoshDDOPopUp.asp?{}'

    query_index = 9


class TreasuryReceiptsSpider(treasury_base.TreasuryBaseSpider):
    '''
    Spider for Treasury Receipts.
    '''
    name = 'treasury_receipts'

    start_urls = ['https://himkosh.hp.nic.in/treasuryportal/eKosh/eKoshDDOReceiptQuery.asp']

    # dataset is collected from here.
    query_url = 'https://himkosh.hp.nic.in/treasuryportal/eKosh/eKoshDDOReceiptPopUp.asp?{}'

    query_index = 1
