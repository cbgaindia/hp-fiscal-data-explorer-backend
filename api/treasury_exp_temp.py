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

@falcon.before(validate_date)
@falcon.before(validate_vis_range)
class TreasuryExpenditureTempVisType():
    '''
    detail exp
    '''
    def on_post(self, req, resp):
        '''
        sample payload
        {"filters": {"major": "2011, 2216", "sub_major": "01, 02"}}
        '''
        params = req.params
        start = datetime.strptime(params['start'], '%Y-%m-%d')
        end = datetime.strptime(params['end'], '%Y-%m-%d')
        vis_range = params['range']

        start_month = params['start'][5:7]
        end_month = params['end'][5:7]
        financial_year = params['start'][0:4]

        req_body = req.stream.read()
        if req_body:
            payload = json.loads(req_body)
        else:
            payload = {}

        if vis_range == 'Week':

            if start.strftime("%A") == 'Sunday':

                week_start_range = start.isocalendar()[1]
            else:

                week_start_range = start.isocalendar()[1]-1


            offset_final = (end.weekday() - 5)%7
            last_saturday_final = end - timedelta(days=offset_final)
            
            if (int(end_month) < int(start_month) or (end_month) == 12):

               end_temp = datetime.strptime(financial_year + '-12-31', '%Y-%m-%d')
               offset_temp = (end_temp.weekday() - 5)%7
               last_saturday_temp = end_temp - timedelta(days=offset_temp)
               week_number = [*range(week_start_range,last_saturday_temp.isocalendar()[1])] + [*range(0,last_saturday_final.isocalendar()[1]+1)]
               
            else:
                
                week_number = [*range(week_start_range,last_saturday_final.isocalendar()[1]+1)]
           

            # if start.strftime("%A") == 'Sunday':
            #     week_number = [*range(start.isocalendar()[1],end.isocalendar()[1]-1)]
            # else:
            #     week_number = [*range(start.isocalendar()[1]-1,end.isocalendar()[1]-1)]

            select = "SELECT Week(DATE(TRANSDATE)),district,sum(GROSS), sum(AGDED), sum(BTDED), sum(NETPAYMENT)"
            from_str = "FROM himachal_pradesh_district_spending_data_desc"
            where = "WHERE TRANSDATE BETWEEN '{}' and '{}'".format(start, end)
            groupby = "GROUP BY district, {}(DATE(TRANSDATE))".format(vis_range)


            for key, value in payload['filters'].items():
                where += "AND {key} IN ({value})".format(key=key, value=value)
            query_string = select + ' ' + from_str + ' ' + where + ' ' + groupby
            print(query_string)
            query = CONNECTION.execute(query_string)
            data_rows = query.fetchall()
            records = []
            
            for row in data_rows:
                records.append(row.values())

            query_week_num = []
            for i in records:
                query_week_num.append(i[0])
            
            dict_hp = {}

            districts = []
            values = []
            list_comp = list(set(week_number + query_week_num))


            for i in records:
                districts.append(i[1])
                values.append(i[2:])

            if len(query_week_num) == 0:
                pass
            else:
                if query_week_num[-1] == 52:
                    for i in range(len(values[0])):
                        values[0][i] = values[0][i]+values[-1][i]




            dict_hp = {}

            for i in districts:
                dict_hp[i] = []

            for i in range(0,len(districts)):
                dict_hp[districts[i]].append([query_week_num[i],values[i][0:]])

            for i in list_comp:
                for key,values in dict_hp.items():
                    if  i not in [j for i in values for j in i]:
                        dict_hp[key].append([i,[0,0,0,0]]) 
                    else:
                        pass
                    dict_hp[key] = sorted(dict_hp[key], key=lambda x: x[0])

            for key,values in dict_hp.items():
                records_temp = []
                for num in range(len(week_number)):
                    for i in values:
                        if week_number[num]== i[0]:
                           records_temp.append(i)
                dict_hp[key] = records_temp
            
            # for key in dict_hp:
            #     dict_hp[key] =[i[1:][0] for i in dict_hp[key]]

            data_response = json.dumps({'records': dict_hp, 'count': len(records)})

            resp.status = falcon.HTTP_200  #pylint: disable=no-member
            resp.body = data_response
        else:

            if end_month <= start_month:
                month_range = [*range(int(start_month),13)] + [*range(1,int(end_month)+1)]
            else:
                month_range = [*range(int(start_month),int(end_month)+1)]

            select = "SELECT Month(DATE(TRANSDATE)),district,sum(GROSS), sum(AGDED), sum(BTDED), sum(NETPAYMENT)"
            from_str = "FROM himachal_pradesh_district_spending_data_desc"
            where = "WHERE TRANSDATE BETWEEN '{}' and '{}'".format(start, end)
            groupby = "GROUP BY district, {}(DATE(TRANSDATE))".format(vis_range)


            for key, value in payload['filters'].items():
               where += "AND {key} IN ({value})".format(key=key, value=value)
            query_string = select + ' ' + from_str + ' ' + where + ' ' + groupby
            
            print(query_string)

            query = CONNECTION.execute(query_string)
            data_rows = query.fetchall()
            records = []

             
            for row in data_rows:
                records.append(row.values())
            
            records_temp = []

            query_month_num = []
            for i in records:
                query_month_num.append(i[0])
            districts = []
            values_record = []
            total_month_number = month_range
            
            
            for i in records:
                districts.append(i[1])
                values_record.append(i[2:])

            dict_hp = {}

            for i in districts:
                dict_hp[i] = []

            for i in range(0,len(districts)):
                dict_hp[districts[i]].append([query_month_num[i],values_record[i][0:]])
            
            for month_num in total_month_number:
                for key,values in dict_hp.items():
                    if  month_num not in [val for rec in values for val in rec]:
                        dict_hp[key].append([month_num,[0,0,0,0]]) 
                    else:
                        pass
                    dict_hp[key] = sorted(dict_hp[key], key=lambda x: x[0])

            for key,values in dict_hp.items():
                records_temp = []
                for num in range(len(total_month_number)):
                    for i in values:
                        if total_month_number[num]== i[0]:
                           records_temp.append(i)
                dict_hp[key] = records_temp

            
            for key in dict_hp:
                dict_hp[key] =[key_[1:][0] for key_ in dict_hp[key]]

            
            pdb.set_trace()    
            data_response = json.dumps({'records': dict_hp, 'count': len(records)})

            resp.status = falcon.HTTP_200  #pylint: disable=no-member
            resp.body = data_response