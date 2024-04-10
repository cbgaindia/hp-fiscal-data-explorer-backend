'''
budget data endpoints
'''
import json
from datetime import datetime, timedelta, date
import falcon
import pdb
from api.db import CONNECTION
from api.utils import validate_date, CORSMiddleware, validate_vis_range
import time
import pandas as pd
from pandas import DataFrame


class TreasuryAccountHeadsTemp():
    '''
    This API will give permutations and combinations of all account heads
    '''
    def on_get(self, req, resp):
        '''
        Method for getting Permutations Combinations of account heads
        '''
        params = req.params
        list_of_keys = [ k for k in params.keys() ]

        query_string = "select distinct concat_ws('+',District,Treasury_Code,Treasury,DDO_Code, DDO_desc,demand, demand_desc, major, major_desc, sub_major, sub_major_desc, minor, minor_desc, sub_minor, sub_minor_desc, Budget,voted_charged, plan_nonplan, SOE, SOE_description) from himachal_pradesh_district_spending_data_desc;"
        print(query_string)

        query = CONNECTION.execute(query_string)

        df = DataFrame(query.fetchall())
        df.to_csv("heads.csv", index = False)
        #df[['District','Treasury_Code','Treasury','DDO_Code', 'DDO_desc','demand', 'demand_desc', 'major', 'major_desc', 'sub_major', 'sub_major_desc', 'minor', 'minor_desc', 'sub_minor', 'sub_minor_desc', 'Budget','voted_charged', 'plan_nonplan', 'SOE', 'SOE_description']] = df[0].str.split(":", n =20)
        # records = []
        # pdb.set_trace()
        # records = [row.values() for row in data_rows]     
        # records_with_desc = [] 

        # for i in range(len(records)):
        #     records_list = []
        #     records_list.append(records[i][0:1])
        #     for j in range(1,15,2):
        #         records_list.append(['-'.join(records[i][j:j+2])])
        #     records_list.append(records[i][15:18])
        #     records_list.append(['-'.join(records[i][18:20])])
        #     records_list = [acc_heads for acc_heads_value in records_list for acc_heads in acc_heads_value]
        #     records_with_desc.append(records_list)
           

        # dict_hp = {}

        # for rows in records_with_desc:
        #     current_level = dict_hp
        #     for acc_heads in rows:
        #         if acc_heads not in current_level:
        #             current_level[acc_heads] = {}
        #         current_level = current_level[acc_heads]
        
        # resp.status = falcon.HTTP_200  #pylint: disable=no-member
        # resp.body = json.dumps({'records':dict_hp}) 