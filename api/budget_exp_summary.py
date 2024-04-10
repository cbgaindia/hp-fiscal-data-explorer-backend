'''
budget data endpoints
'''
import json
from datetime import datetime
import falcon
import pdb
from api.db import CONNECTION
from api.utils import validate_date, CORSMiddleware, validate_vis_range
import time
import pandas as pd

class AllocationExpenditureSummary():
    """Expenditure Summary"""

    def on_get(self, req, resp):
        '''
        handle get requests for expenditure summary.
        '''
        query_string = "select * from himachal_budget_allocation_expenditure_summary"  # pylint: disable=line-too-long
        query = CONNECTION.execute(query_string)
        data_rows = query.fetchall()
        

        records = []
        financial_year = []
        demand = []
        demand_description = []
        values = []

        for row in data_rows:
            records.append(row.values())

        for i in records:
            financial_year.append(i[1])
            demand.append(i[2] + "-" + i[3])
            values.append(i[4:])

        financial_year_dict = {}

        for i in financial_year:
            financial_year_dict[i] = {}

        for i in range(0,len(financial_year)):
            financial_year_dict[financial_year[i]][demand[i]] = values[i]     

        data_response = json.dumps({'records': financial_year_dict, 'count': len(records)})

        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = data_response
