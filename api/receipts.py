'''
receipts data endpoints
'''
import json
from datetime import datetime, timedelta
import falcon
from api.db import CONNECTION
import pdb  
from api.utils import validate_date, CORSMiddleware

@falcon.before(validate_date)
class DetailReceiptsWeek():
    '''
    detail receipts endpoint
    '''
    def on_post(self, req, resp):
        '''
        Receipts data endpoint
        '''
        params = req.params
        start = datetime.strptime(params['start'], '%Y-%m-%d')
        end = datetime.strptime(params['end'], '%Y-%m-%d')
        start_month = params['start'][5:7]
        end_month = params['end'][5:7]
        financial_year = params['start'][0:4]

        req_body = req.stream.read()
        if req_body:
            payload = json.loads(req_body)
        else:
            payload = {}

        if not payload:
            query_string = """
                SELECT sum(Total_Receipt)
                FROM himachal_budget_receipts_data
                WHERE date BETWEEN '{}' and '{}'
                GROUP BY WEEK(DATE(date))
            """
        else:

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
            
            
            select = "SELECT Week(DATE(date)),sum(Total_Receipt)"
            from_str = "FROM himachal_budget_receipts_data_1"
            where = "WHERE date BETWEEN '{}' and '{}'".format(start, end)
            groupby = "GROUP BY WEEK(DATE(date))"

            for key, value in payload['filters'].items():
                where += "AND {key} IN ({value})".format(key=key, value=value)
            query_string = select + ' ' + from_str + ' ' + where + ' ' + groupby

        query = CONNECTION.execute(query_string)
        data_rows = query.fetchall()
        records = []
        query_week_num = []
        values = []

        for row in data_rows:
            records.append(row.values())
        for i in records:
            query_week_num.append(i[0])
            values.append(i[1:])

        records_new = []

        for i in range(len(query_week_num)):
            records_new.append([query_week_num[i],values[i]])


        for i in week_number:
            if  i not in [j for i in records for j in i]:
                records_new.append([i,[0]])

        records_sorted = []
                
        for num in range(len(week_number)):
            for i in records_new:
                if week_number[num] == i[0]:
                   records_sorted.append(i)
            
       
        # for i in records_sorted:
        #     i.pop(0)

        data_response = json.dumps({'records': records_sorted, 'count': len(records)})

        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = data_response


@falcon.before(validate_date)
class DetailReceiptsMonth():
    '''
    detail receipts endpoint
    '''
    def on_post(self, req, resp):
        '''
        Receipts data endpoint
        '''
        params = req.params
        start = datetime.strptime(params['start'], '%Y-%m-%d')
        end = datetime.strptime(params['end'], '%Y-%m-%d')
        start_month = params['start'][5:7]
        end_month = params['end'][5:7]
        financial_year = params['start'][0:4]


        if end_month <= start_month:
            month_range = [*range(int(start_month),13)] + [*range(1,int(end_month)+1)]
        else:
            month_range = [*range(int(start_month),int(end_month)+1)]


        req_body = req.stream.read()
        if req_body:
            payload = json.loads(req_body)
        else:
            payload = {}

        if not payload:
            query_string = """
                SELECT sum(Total_Receipt)
                FROM himachal_budget_receipts_data_1
                WHERE date BETWEEN '{}' and '{}'
                GROUP BY MONTH(DATE(date))
            """
        else:
            select = "SELECT Month(DATE(date)),sum(Total_Receipt)"
            from_str = "FROM himachal_budget_receipts_data_1"
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

        for i in month_range:
            if  i not in [j for i in records for j in i]:
                records.append([i,0])

        records_sorted = []
        
        for num in range(len(month_range)):
            for i in records:
                if month_range[num] == i[0]:
                   records_sorted.append(i)

        for i in records_sorted:
            i.pop(0)

        data_response = json.dumps({'records': records_sorted, 'count': len(records)})

        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = data_response

class ReceiptsAccountHeads():
    '''
    This API will give permutations and combinations of all account heads
    '''
    def on_get(self, req, resp):
        '''
        Method for getting Permutations Combinations of account heads
        '''
        query_string = "select major, major_desc, sub_major,minor,sub_minor, sub_minor_desc from himachal_budget_receipts_data_1 GROUP BY major,major_desc, sub_major ,minor, sub_minor, sub_minor_desc"  # pylint: disable=line-too-long
        query = CONNECTION.execute(query_string)
        data_rows = query.fetchall()
        records = []
        for row in data_rows:
            records.append(row.values())
        records_with_desc = []
        for i in range(len(records)):
            records_list  = []
            records_list.append(['-'.join(records[i][0:2])])
            records_list.append(records[i][2:4])
            records_list.append(['-'.join(records[i][4:6])])
            records_list = [rec_heads for rec_heads_value in records_list for rec_heads in rec_heads_value]
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