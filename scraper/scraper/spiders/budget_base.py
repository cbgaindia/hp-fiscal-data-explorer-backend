'''
Base Spider for budget crawling.
'''
# -*- coding: utf-8 -*-

import csv
import os
import re
from urllib.parse import urlencode

import scrapy
from scrapy import signals
from scrapy.exceptions import CloseSpider
from scrapy.spidermiddlewares.httperror import HttpError

from scraper.settings import BUDGET_DATA_PATH
from scraper.utils import parsing_utils


class BudgetBaseSpider(scrapy.Spider):
    '''
    Base spider for HP budget.
    '''
    allowed_domains = ['himkosh.hp.nic.in', 'himkosh.nic.in']
    query_url = None
    query_index = None
    unit = None

    def parse(self, response):
        '''
        will be implemented in inherited classes.
        '''

    def handle_err(self, failure):
        '''
        Logs the request and response details when a request fails.
        '''
        if failure.check(HttpError):
            response = failure.value.response
            request = response.request
            self.logger.error('Request: {}'.format(request))
            self.logger.error('Request headers: {}'.format(request.headers))
            self.logger.error('Response headers: {}'.format(response.headers))


class ExpenditureBaseSpider(BudgetBaseSpider):
    '''
    Base spider for HP budget expenditure. It has methods for making dataset requests
    for a given HOD code with a given query with given date.
    '''
    def __init__(self, *args, **kwargs):
        super(ExpenditureBaseSpider, self).__init__(*args, **kwargs)
        if not hasattr(self, 'date'):
            raise CloseSpider('No date given!')

        self.date = kwargs.pop('date')
        self.query_id = kwargs.pop('query_id', None)
        self.query_name = kwargs.pop('query_name', None)
        self.hod_name = kwargs.pop('hod_name', None)

    def make_dataset_request(self, params):
        '''
        Construct and yield a scrapy request for a dataset.
        '''
        # generate a filename to store the dataset in.
        filename = '{}_{}.csv'.format(self.name, self.date)

        # directory to store the dataset file, either expenditures or receipts
        budget_data_dir = self.name.split('_')[-1]

        # path to the file to save the records
        dataset_filepath = os.path.join(BUDGET_DATA_PATH, budget_data_dir, filename)

        # don't request the same dataset again if it's already collected previously
        # check if a file with a particular dataset name exist, if it does then
        # also check if it's empty or not, if it's empty we request it again.
        if not os.path.exists(dataset_filepath) or not os.stat(dataset_filepath).st_size:
            query_params = {
                'from_date': params['date'],  # format: yyyymmdd
                'To_date': params['date'],      # format: yyyymmdd
                'ddlquery': params['query_id'],
                'HODCode': params['hod_name'],
                'Str': params['query_name'],
                'Unit' : params['unit']
            }

            return scrapy.Request(
                self.query_url.format(urlencode(query_params)),
                self.parse_dataset,
                errback=self.handle_err,
                meta={'filepath': dataset_filepath}
            )
        self.logger.warning('Dataset file already exists')
        raise CloseSpider('Dataset file already exists')

    def start_requests(self):
        '''
        This method is called when the spider opens.
        It will check if arguments for specific query, HOD code were provided.
        If they were provided then it'll query specifically for that otherwise it goes to
        the expenditures' home page and collects for all the treasuries.
        '''
        if not all(self.__dict__.values()):
            return scrapy.Request(self.start_urls[0], self.parse)
        params = {
            'date': self.date,
            'query_id': self.query_id,
            'query_name': self.query_name,
            'hod_name': self.hod_name,
            'unit': self.unit
        }
        return self.make_dataset_request(params)

    def parse(self, response):
        '''
        Collect queryable params and make dataset queries.
        Parameters to be collected are:

        query_id,
        HODCode,
        query_text
        '''
        # collect details of query 9 that gives consolidated data.
        query_elem = response.xpath('id("ddlQuery")/option')[self.query_index]

        # extract parameters for query i.e. query id and its text.
        query_id = query_elem.xpath('./@value').extract_first()
        query_name = query_elem.xpath('.//text()').extract_first()

        # remove extra whitespaces from query text.
        query_name = parsing_utils.clean_text(query_name)

        # extract parameters for hod_code i.e.its text.
        hod_elem = response.xpath('id("cmbDpt")/option')[0]

        hod_name = hod_elem.xpath('.//@value').extract_first()

        # remove extra whitespaces from query text.
        hod_name = parsing_utils.clean_text(hod_name)

        params = {
            'date': self.date,
            'query_id': query_id,
            'query_name': query_name,
            'hod_name': hod_name,
            'unit': self.unit
            }
        return self.make_dataset_request(params)

    def parse_dataset(self, response):
        '''
        Parse each dataset page to collect the data in a csv file.
        output: a csv file named with budget_expenditures_date format.
        '''
        # header row for the file.
        heads = response.xpath('//table//tr[@class="popupheadingeKosh"]//td//text()').extract()
        # all other rows
        data_rows = response.xpath('//table//tr[contains(@class, "pope")]')

        if not data_rows:
            return

        # prepare file name and its path to write the file.
        filepath = response.meta.get('filepath')

        with open(filepath, 'w') as output_file:
            writer = csv.writer(output_file, delimiter=',')

            # write the header
            writer.writerow(heads)

            # write all other rows
            for row in data_rows:
                cols = row.xpath('.//td')
                observation = []
                for col in cols:
                    # since we need consistency in the row length,
                    # we need to extract each cell and set empty string when no data found.
                    # by default scrapy omits the cell if it's empty and it can cause inconsistent row lengths.  # pylint:disable=line-too-long
                    observation.append(col.xpath('./text()').extract_first(' '))
                writer.writerow(observation)


class ReceiptBaseSpider(BudgetBaseSpider):
    '''
    Base spider for HP budget receipts.
    '''
    def __init__(self, *args, **kwargs):
        super(ReceiptBaseSpider, self).__init__(*args, **kwargs)
        if not hasattr(self, 'date'):
            raise CloseSpider('No date given!')

        self.date = kwargs.pop('date')

        date_format_pattern = r'\d{2}/\d{2}/\d{4}'
        if not re.match(date_format_pattern, self.date):
            raise CloseSpider('Wrong date format, please use dd/mm/yyyy!')

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ReceiptBaseSpider, cls).from_crawler(crawler, *args, **kwargs)

        # NOTE: Ref-https://docs.scrapy.org/en/latest/topics/signals.html#spider-closed
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        '''
        use to close globally open dataset file after all major code requests are finished.
        '''
        if hasattr(self, 'dataset_file'):
            self.dataset_file.close()
        spider.logger.info('Spider closed: %s', spider.name)

    def make_dataset_request(self, url, callback, params):
        '''
        Construct and yield a scrapy request for a dataset.
        '''
        payload = {
            'fromDate': params['date'],  # format: dd/mm/yyyy
            'toDate': params['date'],      # format: dd/mm/yyyy
            'isPublicHead': params['public_head_bool'],  # 0: Receipt Heads, 1: Public Heads
            'majCd': params['major_code'],
            'Unit' : params['unit']
        }
        # NOTE: need to convert it to a JSON request to get json response.
        return scrapy.FormRequest(
            url,
            callback,
            formdata=payload,
            errback=self.handle_err,
            meta={'csv_writer': params['csv_writer'],
                  'public_head_bool': params['public_head_bool']}
        )

    def start_requests(self):
        '''
        This method is called when the spider opens.
        It will check if arguments for public head/receipt head, major code were provided.
        If they were provided then it'll query specifically for that otherwise it uses some default.

        It also opens a dataset csv file on an instance level which is closed by
        spider_closed signal.
        This is done to prevent overwriting of the same file whenever a request for a major code
        is made.
        '''
        # generate a filepath to store the dataset in.
        # here we replace '/' with '_' to avoid name issues due to date format: dd/mm/yyyy
        filename = '{}_{}.csv'.format(self.name, self.date.replace('/', '_'))
        filepath = os.path.join(BUDGET_DATA_PATH, self.name.split('_')[-1], filename)

        # don't request the same dataset again if it's already collected previously
        # check if a file with a particular dataset name exist, if it does then
        # also check if it's empty or not, if it's empty we request it again.
        # NOTE: here we will now need to check if the file contains only header or more data rows.
        # since the file is being opened globally and we create a header row irrespective of the
        # following requests being handled, there will always be a file with one header row.
        if not os.path.exists(filepath) or not os.stat(filepath).st_size:
            self.dataset_file = open(filepath, 'w')

            heads = ['Maj-Smj-Min-Smn [Sub Minor Head]', 'date', 'Total Receipt']
            csv_writer = csv.writer(self.dataset_file, delimiter=',')
            csv_writer.writerow(heads)

            for public_head_bool in ['0', '1']:
                params = {
                    'date': self.date,
                    'unit': self.unit,
                    'public_head_bool': public_head_bool,
                    'major_code': '0000',  # code to select all major heads
                    'csv_writer': csv_writer
                }
                yield self.make_dataset_request(self.query_url, self.parse_major_codes, params)

        else:
            self.logger.warning('Dataset file already exists')
            raise CloseSpider('Dataset file already exists')

    def parse_major_codes(self, response):
        '''
        parse major codes to make further requests.
        '''
        response.selector.remove_namespaces()

        public_head_bool = response.meta.get('public_head_bool')
        major_codes = response.xpath('//ReceiptMajorDetail/Major/text()').re(r'(\d+)')

        request_url = 'https://himkosh.nic.in/eHPOLTIS/PublicReports/Receipt.asmx/GetReceiptSubMinorDetail'  #pylint: disable=line-too-long

        for major_code in major_codes:
            params = {
                'date': self.date,
                'unit': self.unit,
                'public_head_bool': public_head_bool,
                'major_code': major_code,
                'csv_writer': response.meta.get('csv_writer')
            }
            yield self.make_dataset_request(request_url, self.parse_dataset, params)

    def parse_dataset(self, response):
        '''
        parse receipts data.
        '''
        response.selector.remove_namespaces()

        # all other rows
        data_rows = response.xpath('//ReceiptSubMinorDetail')

        if not data_rows:
            self.logger.warning('No dataset found for {}'.format(response.url))
            return

        # get the csv writer from meta
        writer = response.meta.get('csv_writer')

        # write all other rows
        for row in data_rows:
            cols = [x for x in row.xpath('.//text()').re('.+') if x.strip()]
            cols.insert(1, self.date)
            writer.writerow(cols)
