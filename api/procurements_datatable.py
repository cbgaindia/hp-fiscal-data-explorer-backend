import json
import datetime
import falcon
import pdb
from api.db import CONNECTION
from api.utils import validate_date, CORSMiddleware, validate_vis_range
import time
import pandas as pd
import numpy
from json import JSONEncoder

class TenderDatatable():
    """Covid"""

    def on_get(self, req, resp):
        '''
        handle get requests for expenditure summary.
        '''
        query_string = "select * from himachal_tender_datatable where `tender/classification/id`!=''"  # pylint: disable=line-too-long
        query = CONNECTION.execute(query_string)
        data_rows = query.fetchall()
        
        records = []

        list_of_columns = ["uuid","ocid","id","date","tender/id","tag", "tender/participationFees/0/value/currency","tender/participationFees/0/value/amount","tender/participationFees/0/paymentAddress/locality","tender/datePublished",
                        "tender/awardPeriod/startDate","tender/documentAvailabilityPeriod/startDate","tender/documentAvailabilityPeriod/endDate","tender/enquiryPeriod/startDate","tender/enquiryPeriod/endDate","tender/tenderPeriod/startDate","tender/tenderPeriod/endDate","tender/title",
                        "tender/description","tender/value/currency","tender/value/amount",
                        "tender/items/id","tender/items/description","initiationType","tender/process",
                        "tender/tenderPeriod/durationInDays","tender/contractPeriod/durationInDays","parties/0/address/locality","tender/bidOpening/address/streetAddress","tender/allowPreferentialBidder","tender/participationFees/1/value/currency","tender/participationFees/1/value/amount","tender/participationFees/1/exemptionAllowed","tender/participationFees/1/description","tender/participationFees/0/payee/name","buyer/id",
                        "buyer/name","tender/externalReference","tender/procurementMethod","tender/contractType","tender/mainProcurementCategory","tender/evaluation/generalTechnicalEvaluationAllowed","tender/evaluation/itemWiseTechnicalEvaluationAllowed","tender/participationFee/0/multiCurrencyAllowed","tender/allowTwoStageBid","tender/procuringEntity/id","tender/procuringEntity/name","parties/0/id","parties/0/roles","parties/0/contactPoint/name","parties/0/address/streetAddress","parties/0/address/postalCode","tender/participationFees/0/description","tender/participationFees/0/methodOfPayment","award/date","tender/numberOfTenderers","tender/classification/scheme","tender/classification/id","tender/classification/description"]

        for row in data_rows:
            records.append(row.values())
      
        tender_array = numpy.array(records)

        procurement_tender_datatable = {}
    
        for index,column_names in enumerate(list_of_columns):
            procurement_tender_datatable[column_names] =  tender_array[:,index].tolist()  
        
        del procurement_tender_datatable['uuid']

        data_response = json.dumps({'records': procurement_tender_datatable, 'count': len(records)}, default=str)

        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = data_response

class AwardsDatatable():
    """Covid"""

    def on_get(self, req, resp):
        '''
        handle get requests for expenditure summary.
        '''
        query_string = "select * from himachal_awards_datatable LEFT JOIN himachal_tender_datatable ON himachal_tender_datatable.ocid = himachal_awards_datatable.ocid where himachal_tender_datatable.`tender/classification/id`!='';"  # pylint: disable=line-too-long
        query = CONNECTION.execute(query_string)
        data_rows = query.fetchall()

        records = []

        list_of_columns = ["uuid","ocid","id","date","parties/0/roles","awards/0/id","awards/0/suppliers/0/id","parties/0/id","parties/0/contactPoint/name",
                          "awards/0/suppliers/0/name","awards/0/value/currency","awards/0/value/amount"]

        for row in data_rows:
            records.append(row.values())
      
        tender_array = numpy.array(records)

        procurement_awards_datatable = {}
    
        for index,column_names in enumerate(list_of_columns):
            procurement_awards_datatable[column_names] =  tender_array[:,index].tolist()  
        
        del procurement_awards_datatable['uuid']

        data_response = json.dumps({'records': procurement_awards_datatable, 'count': len(records)}, default=str)

        resp.status = falcon.HTTP_200  #pylint: disable=no-member
        resp.body = data_response