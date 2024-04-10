'''
Spider for collecting ddo codes.
'''
import csv
import os
from datetime import datetime as dt
from urllib.parse import parse_qs, urlencode

import scrapy

from scraper.settings import SPENDING_DATA_PATH


class DDOCodeCollector(scrapy.Spider):
    '''
    Collect DDO Codes for treasuries.
    '''
    name = 'ddo_collector'
    start_urls = ['https://himkosh.nic.in/eHPOLTIS/PublicReports/wfrmDDOAllocationExpenditure.aspx']

    def parse(self, response):
        # create form request that will automatically collect form data from the page response.
        form_req = scrapy.FormRequest.from_response(response, callback=self.collect_ddo_code)

        # convert the form body to dictionary
        formdata = parse_qs(form_req.body)

        # remove the param that will collect datasets.
        # we are only interested in ddo codes which will get from ajax response.
        formdata.pop(b'ctl00$MainContent$btnGetReport')

        # values collected in the form dictionary are in list form,
        # make them strings.
        formdata = {key: value[0] for key, value in formdata.items()}

        # we need ddo codes for all treasuries, so collect treasury codes from dropdown.
        treasuries = response.xpath('id("ddlTreaCode")/option')
        for treasury in treasuries[1:]:
            treasury_code = treasury.xpath('./@value').extract_first()

            # now for every treasury code make the ajax request that'll fetch the ddos.
            formdata[b'ctl00$MainContent$ddlTreaCode'] = treasury_code
            yield form_req.replace(body=urlencode(formdata),
                                   meta={'treasury': treasury_code})

    def collect_ddo_code(self, response):
        '''
        create a CSV file with DDO names and DDO Codes.
        '''
        ddo_selector = response.xpath('id("ddlDDOCode")/option')

        treasury_code = response.meta.get('treasury')

        dir_path = os.path.join(SPENDING_DATA_PATH, '{}_ddo_codes'.format(treasury_code))
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

        filepath = os.path.join(
            dir_path,
            '{}_ddo_codes_{}.csv'.format(treasury_code, dt.today().strftime('%Y-%m-%d'))
        )

        with open(filepath, 'w') as output_file:
            writer = csv.writer(output_file, delimiter=',')
            writer.writerow(['DDO Code', 'DDO Name'])

            for row in ddo_selector[1:]:
                code = row.xpath('./@value').extract_first()
                name = row.xpath('./text()').extract_first()
                writer.writerow([code, name])
