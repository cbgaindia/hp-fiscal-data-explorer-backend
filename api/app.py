'''
api definition
'''

import falcon

from api import budget_ep, receipts, schemes, treasury_receipts, budget_exp_summary,treasury_exp_temp,budget_heads_temp,covid_spending, procurements_datatable

# create API
api = app = falcon.API(middleware=[budget_ep.CORSMiddleware(), receipts.CORSMiddleware()])

# create endpoints for API.
api.add_route('/api/exp_summary', budget_ep.ExpenditureSummary())
api.add_route('/api/detail_exp_week', budget_ep.DetailExpenditureWeek())
api.add_route('/api/detail_exp_month', budget_ep.DetailExpenditureMonth())
api.add_route('/api/acc_heads', budget_ep.DetailAccountHeads())
api.add_route('/api/acc_heads_test', budget_ep.DetailAccountHeadsTest())
api.add_route('/api/acc_heads_desc', budget_ep.DetailAccountHeadsDesc())
api.add_route('/api/unique_acc_heads', budget_ep.UniqueAccountHeads())
api.add_route('/api/unique_acc_heads_treasury', budget_ep.UniqueAccountHeadsTreasury())
api.add_route('/api/unique_acc_heads_treasury_rec', treasury_receipts.UniqueAccountHeadsTreaReceipts())
api.add_route('/api/unique_acc_heads_schemes', schemes.UniqueAccountHeadsSchemes())
api.add_route('/api/detail_receipts_week', receipts.DetailReceiptsWeek())
api.add_route('/api/detail_receipts_month', receipts.DetailReceiptsMonth())
api.add_route('/api/treasury_exp', budget_ep.TreasuryExpenditureVisType())
api.add_route('/api/treasury_rec', treasury_receipts.TreasuryReceiptsVisType())
api.add_route('/api/treasury_exp_week', budget_ep.TreasuryExpenditureWeek())
api.add_route('/api/acc_heads_treasury', budget_ep.TreasuryAccountHeads())
api.add_route('/api/acc_heads_treasury_rec', treasury_receipts.TreasuryReceiptsAccountHeads())
api.add_route('/api/acc_heads_receipts', receipts.ReceiptsAccountHeads())
api.add_route('/api/acc_heads_schemes',schemes.SchemesAccountHeads())
api.add_route('/api/schemes', schemes.SchemesVisType())
api.add_route('/api/allocation_exp_summary', budget_exp_summary.AllocationExpenditureSummary())

#temprory APIs
api.add_route('/api/treasury_exp_temp', treasury_exp_temp.TreasuryExpenditureTempVisType())
api.add_route('/api/heads_temp', budget_heads_temp.TreasuryAccountHeadsTemp())


#covid

api.add_route('/api/covid_summary', covid_spending.CovidSpending())

#procurements

api.add_route('/api/procurement_tenders',procurements_datatable.TenderDatatable())
api.add_route('/api/procurement_awards',procurements_datatable.AwardsDatatable())