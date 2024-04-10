import json
from datetime import datetime
import falcon
import pdb
from api.db import CONNECTION
from api.utils import validate_date, CORSMiddleware, validate_vis_range
import time
import pandas as pd

class CovidSpending():
    """Covid"""

    def on_get(self, req, resp):
        '''
        handle get requests for expenditure summary.
        '''
        query_string = "select * from himachal_covid_table"  # pylint: disable=line-too-long
        query = CONNECTION.execute(query_string)
        data_rows = query.fetchall()
        

        records = []
        name_of_department_distrits_organisation = []
        Funds_Sanctioned = []
        Funds_Sanctioned_From = []
        Date  = []
        Purpose = []


        for row in data_rows:
            records.append(row.values())

        for i in records:
            name_of_department_distrits_organisation.append(i[2])
            Funds_Sanctioned.append(i[3])
            Funds_Sanctioned_From.append(i[4])
            Date.append(i[5])
            Purpose.append(i[6])

        covid_spending_dict = {}

        
        covid_spending_dict['name_of_department_distrits_organisation'] = name_of_department_distrits_organisation
        covid_spending_dict['Funds_Sanctioned'] = Funds_Sanctioned
        covid_spending_dict['Funds_Sanctioned_From'] = Funds_Sanctioned_From
        covid_spending_dict['Date'] = Date
        covid_spending_dict['Purpose'] = Purpose




        # response_data = {'records': []}
        # for row in data_rows:
        #     record = {}
        #     record['name_of_department_distrits_organisation'] = row[2]
        #     record['Funds_Sanctioned'] = row[3]
        #     record['Funds_Sanctioned_From'] = row[4]
        #     record['Date'] = row[5]
        #     record['Purpose'] = row[6]
        #     response_data['records'].append(record)


        data_response = json.dumps({'records': covid_spending_dict, 'count': len(records)})

        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = data_response

