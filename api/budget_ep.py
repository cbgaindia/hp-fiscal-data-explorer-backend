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


class ExpenditureSummary():
    """Expenditure Summary"""

    def on_get(self, req, resp):
        '''
        handle get requests for expenditure summary.
        '''
        query_string = "select * from himachal_budget_expenditure_summary"  # pylint: disable=line-too-long
        query = CONNECTION.execute(query_string)
        data_rows = query.fetchall()

        response_data = {'records': []}
        for row in data_rows:
            record = {}
            record['demand'] = row[1]
            record['demand_description'] = row[2]
            record['sanction_previous'] = row[3]
            record['sanction_current'] = row[4]
            record['pct_change'] = row[5]
            response_data['records'].append(record)


        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = json.dumps(response_data)

@falcon.before(validate_date)
class DetailExpenditureWeek():
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

        req_body = req.stream.read()
        if req_body:
            payload = json.loads(req_body)
        else:
            payload = {}

        if not payload:
            query_string = """
                SELECT sum(SANCTION), sum(ADDITION), sum(SAVING), sum(REVISED)
                FROM himachal_budget_allocation_expenditure
                WHERE date BETWEEN '{}' and '{}'
                GROUP BY WEEK(DATE(date))
            """
        else:
            select = "SELECT sum(SANCTION), sum(ADDITION), sum(SAVING), sum(REVISED)"
            from_str = "FROM himachal_budget_allocation_expenditure"
            where = "WHERE date BETWEEN '{}' and '{}'".format(start, end)
            groupby = "GROUP BY WEEK(DATE(date))"

            for key, value in payload['filters'].items():
                where += "AND {key} IN ({value})".format(key=key, value=value)
            query_string = select + ' ' + from_str + ' ' + where + ' ' + groupby
        query = CONNECTION.execute(query_string)
        data_rows = query.fetchall()
        records = []
        for row in data_rows:
            records.append(row.values())
        data_response = json.dumps({'records': records, 'count': len(records)})

        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = data_response

@falcon.before(validate_date)
class DetailExpenditureMonth():
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

        req_body = req.stream.read()
        if req_body:
            payload = json.loads(req_body)
        else:
            payload = {}

        if not payload:
            query_string = """
                SELECT sum(SANCTION), sum(ADDITION), sum(SAVING), sum(REVISED)
                FROM himachal_budget_allocation_expenditure
                WHERE date BETWEEN '{}' and '{}'
                GROUP BY MONTH(DATE(date))
            """
        else:
            select = "SELECT sum(SANCTION), sum(ADDITION), sum(SAVING), sum(REVISED)"
            from_str = "FROM himachal_budget_allocation_expenditure"
            where = "WHERE date BETWEEN '{}' and '{}'".format(start, end)
            groupby = "GROUP BY MONTH(DATE(date))"

            for key, value in payload['filters'].items():
                where += "AND {key} IN ({value})".format(key=key, value=value)
            query_string = select + ' ' + from_str + ' ' + where + ' ' + groupby

        query = CONNECTION.execute(query_string)
        data_rows = query.fetchall()
        records = []
        
        for row in data_rows:
            records.append(row.values())
        data_response = json.dumps({'records': records, 'count': len(records)})

        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = data_response

@falcon.before(validate_date)
@falcon.before(validate_vis_range)
class TreasuryExpenditureVisType():
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

            

            data_response = json.dumps({'records': dict_hp, 'count': len(records)})

            resp.status = falcon.HTTP_200  #pylint: disable=no-member
            resp.body = data_response


@falcon.before(validate_date)
class TreasuryExpenditureWeek():
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

        req_body = req.stream.read()
        if req_body:
            payload = json.loads(req_body)
        else:
            payload = {}

        if not payload:
            query_string = """
                SELECT sum(GROSS), sum(AGDED), sum(BTDED), sum(NETPAYMENT)
                FROM himachal_pradesh_district_spending_data_desc
                WHERE date BETWEEN '{}' and '{}'
                GROUP BY WEEK(DATE(TRANSDATE))
            """
        else:
            select = "SELECT sum(GROSS), sum(AGDED), sum(BTDED), sum(NETPAYMENT)"
            from_str = "FROM himachal_pradesh_district_spending_data_desc"
            where = "WHERE TRANSDATE BETWEEN '{}' and '{}'".format(start, end)
            groupby = "GROUP BY WEEK(DATE(TRANSDATE))"

            for key, value in payload['filters'].items():
                where += "AND {key} IN ('{value}')".format(key=key, value=value)
            query_string = select + ' ' + from_str + ' ' + where + ' ' + groupby

        query = CONNECTION.execute(query_string)
        data_rows = query.fetchall()
        records = []
    
        for row in data_rows:
            records.append(row.values())
        data_response = json.dumps({'records': records, 'count': len(records)})

        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = data_response

class DetailAccountHeads():
    '''
    This API will give permutations and combinations of all account heads
    '''
    def on_get(self, req, resp):
        '''
        Method for getting Permutations Combinations of account heads
        '''
        query_string = "select demand,major,sub_major,minor,sub_minor, budget, voted_charged, plan_nonplan, SOE from himachal_budget_allocation_expenditure GROUP BY demand, major, sub_major ,minor, sub_minor, budget, voted_charged, plan_nonplan, SOE"  # pylint: disable=line-too-long
        query = CONNECTION.execute(query_string)
        data_rows = query.fetchall()
        records = []


        for row in data_rows:
            records.append(row.values())

        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = json.dumps({'records':records, 'count': len(records)})

class DetailAccountHeadsTest():
    '''
    This API will give permutations and combinations of all account heads
    '''
    def on_get(self, req, resp):
        '''
        Method for getting Permutations Combinations of account heads
        '''
        query_string = "select demand,major,sub_major,minor,sub_minor, budget, voted_charged, plan_nonplan, SOE from himachal_budget_allocation_expenditure GROUP BY demand, major, sub_major ,minor, sub_minor, budget, voted_charged, plan_nonplan, SOE"  # pylint: disable=line-too-long
        query = CONNECTION.execute(query_string)
        data_rows = query.fetchall()
        records = []
        for row in data_rows:
            records.append(row.values())        
        dict_hp = {}

        for rows in records:
            current_level = dict_hp
            for acc_heads in rows:
                if acc_heads not in current_level:
                    current_level[acc_heads] = {}
                current_level = current_level[acc_heads]

        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = json.dumps({'records':dict_hp})

class DetailAccountHeadsDesc():
    '''
    This API will give permutations and combinations of all account heads
    '''
    def on_get(self, req, resp):
        '''
        Method for getting Permutations Combinations of account heads
        '''
        query_string = "select demand,demand_desc,major, major_desc, sub_major, sub_major_desc, minor,minor_desc,sub_minor,sub_minor_desc, budget, voted_charged, plan_nonplan, SOE, SOE_description from himachal_budget_allocation_expenditure_data_desc GROUP BY demand,demand_desc,major,major_desc,sub_major,sub_major_desc,minor,minor_desc,sub_minor,sub_minor_desc, budget, voted_charged, plan_nonplan, SOE, SOE_description"  # pylint: disable=line-too-long
        query = CONNECTION.execute(query_string)
        data_rows = query.fetchall()
       
        records = []
        records = [row.values() for row in data_rows]  
        
        records_with_desc = [] 
        for i in range(len(records)):
            records_list = []
            for j in range(0,10,2):
                records_list.append(['-'.join(records[i][j:j+2])])
                
            records_list.append(records[i][10:13])
            records_list.append(['-'.join(records[i][13:15])])
            records_list = [acc_heads for acc_heads_value in records_list for acc_heads in acc_heads_value]
            records_with_desc.append(records_list)
        
        dict_hp = {}

        for rows in records_with_desc:
            current_level = dict_hp
            for acc_heads in rows:
                if acc_heads not in current_level:
                    current_level[acc_heads] = {}
                current_level = current_level[acc_heads]
        
        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = json.dumps({'records':dict_hp})

class UniqueAccountHeads():
    '''
    This API will give permutations and combinations of all account heads
    '''
    def on_get(self, req, resp):
        '''
        Method for getting Permutations Combinations of account heads
        '''
        query_string = "SELECT `COLUMN_NAME`  FROM `INFORMATION_SCHEMA`.`COLUMNS`  WHERE `TABLE_SCHEMA`='himachal_pradesh_data' AND `TABLE_NAME`='himachal_budget_allocation_expenditure'"  # pylint: disable=line-too-long
        get_column_names = CONNECTION.execute(query_string)
        column_names = get_column_names.fetchall()
        column_names_list  =  [row.values() for row in column_names]     
        target_index = column_names_list.index(['SOE'])
        list_acc_heads = column_names_list[1:target_index+1]
      
        dict_unique_acc_heads = {}
        for acc_heads in list_acc_heads:
            query_select = "select distinct {} from himachal_budget_allocation_expenditure".format(acc_heads[0])
            query_unique_acc_heads = CONNECTION.execute(query_select)
            unique_acc_heads_value = query_unique_acc_heads.fetchall()
            unique_acc_heads_value_list =  [row_acc.values() for row_acc in unique_acc_heads_value] 
            unique_acc_heads_value_list = [acc_heads for acc_heads_value in unique_acc_heads_value_list for acc_heads in acc_heads_value]
            dict_unique_acc_heads[acc_heads[0]] = unique_acc_heads_value_list
        
        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = json.dumps(dict_unique_acc_heads)

class TreasuryAccountHeads():
    '''
    This API will give permutations and combinations of all account heads
    '''
    def on_get(self, req, resp):
        '''
        Method for getting Permutations Combinations of account heads
        '''
        params = req.params
        list_of_keys = [ k for k in params.keys() ]
        select = "SELECT District,Treasury_Code,Treasury, DDO_Code, DDO_Desc, demand, demand_desc, major, major_desc, sub_major, sub_major_desc, minor, minor_desc, sub_minor, sub_minor_desc,budget, voted_charged, plan_nonplan, SOE, SOE_description"
        from_str = " FROM himachal_pradesh_district_spending_data_desc"
        where = "where "
        groupby = "GROUP BY District, Treasury_Code,Treasury, DDO_Code, DDO_Desc,demand, demand_desc, major, major_desc, sub_major, sub_major_desc, minor, minor_desc, sub_minor, sub_minor_desc,budget, voted_charged,plan_nonplan, SOE, SOE_description"
        
        for key, value in params.items():
            where += "{key} IN ({value}) AND ".format(key=key, value=value)
        query_string = select + ' ' + from_str + ' ' + where[:len(where)-4] + ' ' + groupby

        query = CONNECTION.execute(query_string)
        data_rows = query.fetchall()
        records = []

        records = [row.values() for row in data_rows]     
        records_with_desc = [] 

        for i in range(len(records)):
            records_list = []
            records_list.append(records[i][0:1])
            for j in range(1,15,2):
                records_list.append(['-'.join(records[i][j:j+2])])
            records_list.append(records[i][15:18])
            records_list.append(['-'.join(records[i][18:20])])
            records_list = [acc_heads for acc_heads_value in records_list for acc_heads in acc_heads_value]
            records_with_desc.append(records_list)
           

        dict_hp = {}

        for rows in records_with_desc:
            current_level = dict_hp
            for acc_heads in rows:
                if acc_heads not in current_level:
                    current_level[acc_heads] = {}
                current_level = current_level[acc_heads]
        
        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = json.dumps({'records':dict_hp})  

class UniqueAccountHeadsTreasury():
    '''
    This API will give permutations and combinations of all account heads
    '''
    def on_get(self, req, resp):
        '''
        Method for getting Permutations Combinations of account heads
        '''
        # query_string = "SELECT `COLUMN_NAME`  FROM `INFORMATION_SCHEMA`.`COLUMNS`  WHERE `TABLE_SCHEMA`='himachal_pradesh_data' AND `TABLE_NAME`='himachal_pradesh_district_spending_data_desc'"  # pylint: disable=line-too-long
        # get_column_names = CONNECTION.execute(query_string)
        # column_names = get_column_names.fetchall()
        
        # column_names_list  =  [row.values() for row in column_names]     
        
        # list_acc_heads_with_desc =column_names_list[2:16] + column_names_list[19:21]
        
        # list_acc_heads_without_desc = column_names_list[1:2] + column_names_list[16:19]
        # list_acc_heads_without_desc = [acc_heads for acc_heads_value in list_acc_heads_without_desc  for acc_heads in acc_heads_value]
        dict_unique_acc_heads = {}

        table_name = ['treasury_unique_heads',
                      'ddo_unique_heads','demand_unique_heads',
                      'major_unique_heads','sub_major_unique_heads',
                      'minor_unique_heads','sub_minor_unique_heads','SOE_unique_heads','district_unique_heads',
                      'budget_unique_heads','v_c_unique_heads','p_n_unique_heads']

        column_names_new = ['Treasury_Code','DDO_Code','demand','major','sub_major','minor','sub_minor','SOE','District',
                            'budget','voted_charged','plan_nonplan']
        
        
        for acc_heads_index in range(0,12):
            query_select = "select {} from {}".format(column_names_new[acc_heads_index],table_name[acc_heads_index])
            query_unique_acc_heads = CONNECTION.execute(query_select)
            unique_acc_heads_value = query_unique_acc_heads.fetchall()
            unique_acc_heads_value_list =  [row_acc.values() for row_acc in unique_acc_heads_value] 
            unique_acc_heads_value_list = [acc_heads for acc_heads_value in unique_acc_heads_value_list for acc_heads in acc_heads_value]
            dict_unique_acc_heads[column_names_new[acc_heads_index]] = unique_acc_heads_value_list


        # for acc_heads_index in range(0,15,2):
        #     query_select = "select distinct concat_ws('-',{},{}) from himachal_pradesh_district_spending_data_desc".format(list_acc_heads_with_desc[acc_heads_index][0],list_acc_heads_with_desc[acc_heads_index+1][0])
        #     query_unique_acc_heads = CONNECTION.execute(query_select)
        #     unique_acc_heads_value = query_unique_acc_heads.fetchall()
        #     unique_acc_heads_value_list =  [row_acc.values() for row_acc in unique_acc_heads_value] 
        #     unique_acc_heads_value_list = [acc_heads for acc_heads_value in unique_acc_heads_value_list for acc_heads in acc_heads_value]
        #     dict_unique_acc_heads[list_acc_heads_with_desc[acc_heads_index][0]] = unique_acc_heads_value_list

        # for acc_heads_index in range(0,4):
        #     query_select = "select distinct {} from himachal_pradesh_district_spending_data_desc".format(list_acc_heads_without_desc[acc_heads_index])
        #     query_unique_acc_heads = CONNECTION.execute(query_select)
        #     unique_acc_heads_value = query_unique_acc_heads.fetchall()
        #     unique_acc_heads_value_list =  [row_acc.values() for row_acc in unique_acc_heads_value] 
        #     unique_acc_heads_value_list = [acc_heads for acc_heads_value in unique_acc_heads_value_list for acc_heads in acc_heads_value]
        #     dict_unique_acc_heads[list_acc_heads_without_desc[acc_heads_index]] = unique_acc_heads_value_list
        # pdb.set_trace()
        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = json.dumps(dict_unique_acc_heads)
