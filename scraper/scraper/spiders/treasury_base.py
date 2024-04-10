# -*- coding: utf-8 -*-
'''
Base Spider for treasury crawling.
'''

import csv
import glob
import os
import time
from urllib.parse import urlencode

import scrapy
from scrapy.exceptions import CloseSpider
from scrapy.spidermiddlewares.httperror import HttpError

from scraper.settings import SPENDING_DATA_PATH
from scraper.utils import parsing_utils

DATASET_PAGE_ERROR_MSG = 'There is no record with given values'


class TreasuryBaseSpider(scrapy.Spider):
    '''
    Base spider for HP treasury. It has methods for making dataset requests
    for a given treasury's given ddo with a given query in a given date range.
    '''
    allowed_domains = ['himkosh.hp.nic.in']

    query_url = None
    query_index = None

    def __init__(self, *args, **kwargs):
        super(TreasuryBaseSpider, self).__init__(*args, **kwargs)
        if not hasattr(self, 'start') and not hasattr(self, 'end'):
            raise CloseSpider('No date range given!')

        self.start = kwargs.pop('start')
        self.end = kwargs.pop('end')
        self.query_id = kwargs.pop('query_id', None)
        self.query_name = kwargs.pop('query_name', None)
        self.treasury_id = kwargs.pop('treasury_id', None)
        self.treasury_name = kwargs.pop('treasury_name', None)
        self.ddo_code = kwargs.pop('ddo_code', None)

    def make_dataset_request(self, params):
        '''
        Construct and yield a scrapy request for a dataset.
        '''
        # generate a filename to store the dataset in.
        filename = parsing_utils.make_dataset_file_name({
            'query': self.name, 'treasury': params['treasury_name'],
            'ddo': params['ddo_code'], 'date': '{}-{}'.format(params['start'], params['end'])
        })

        # directory to store the dataset file, either expenditures or receipts
        spending_data_dir = self.name.split('_')[-1]

        # path to the file to save the records
        dataset_filepath = os.path.join(SPENDING_DATA_PATH, spending_data_dir, filename)

        # don't request the same dataset again if it's already collected previously
        # check if a file with a particular dataset name exist, if it does then
        # also check if it's empty or not, if it's empty we request it again.
        if not os.path.exists(dataset_filepath) or not os.stat(dataset_filepath).st_size:

            treasury_id = params['treasury_id']
            query_params = {
                'from_date': params['start'],  # format: yyyymmdd
                'To_date': params['end'],      # format: yyyymmdd
                'ddlquery': params['query_id'],
                'HODCode': '{}-{}'.format(treasury_id, params['ddo_code']),
                'Str': params['query_name']
            }

            return scrapy.Request(
                self.query_url.format(urlencode(query_params)),
                self.parse_dataset,
                errback=self.handle_err,
                meta={'filepath': dataset_filepath, 'treasury_id': treasury_id}
            )
        return None

    def start_requests(self):
        '''
        This method is called when the spider opens.
        It will check if arguments for specific query, treasury were provided.
        If they were provided then it'll query specifically for that otherwise it goes to
        the expenditures' home page and collects for all the treasuries.
        '''
        if not all(self.__dict__.values()):
            yield scrapy.Request(self.start_urls[0], self.parse)
        else:
            params = {
                'start': self.start,
                'end': self.end,
                'query_id': self.query_id,
                'query_name': self.query_name,
                'treasury_id': self.treasury_id,
                'treasury_name': self.treasury_name,
                'ddo_code': self.ddo_code
            }
            yield self.make_dataset_request(params)

    def parse(self, response):
        '''
        Collect queryable params and make dataset queries.
        Parameters to be collected are:

        query_id,
        HODCode: constructed by joining treasury_code-ddo_code
        query_text
        '''
        # collect details of query 10 that gives consolidated data.
        query_elem = response.xpath('id("ddlQuery")/option')[self.query_index]

        # extract parameters for query i.e. query id and its text.
        query_id = query_elem.xpath('./@value').extract_first()
        query_name = query_elem.xpath('.//text()').extract_first()

        # remove extra whitespaces from query text.
        query_name = parsing_utils.clean_text(query_name)

        # collect all treasury names from dropdown.
        treasuries = response.xpath('id("cmbHOD")/option')

        # for each treasury for each ddo, make requests for datasets for the given date range and query.  # pylint: disable=line-too-long
        for treasury in treasuries[1:]:
            treasury_id = treasury.xpath('./@value').extract_first()
            treasury_name = treasury.xpath('.//text()').extract_first()
            treasury_name = parsing_utils.clean_text(treasury_name)

            self.crawler.stats.set_value('{}/dataset_not_avail_count'.format(treasury_id), 0)
            self.crawler.stats.set_value('{}/dataset_count'.format(treasury_id), 0)

            for ddo_code in self.get_ddo_codes(treasury_id):

                # add a sleep of 1 second before each ddo crawled
                time.sleep(1)

                params = {
                    'start': self.start,
                    'end': self.end,
                    'query_id': query_id,
                    'treasury_id': treasury_id,
                    'treasury_name': treasury_name,
                    'ddo_code': ddo_code,
                    'query_name': query_name
                }
                yield self.make_dataset_request(params)

            # add a sleep of 5 seconds after each treasury crawled
            time.sleep(5)

    def parse_dataset(self, response):
        '''
        Parse each dataset page to collect the data in a csv file.
        output: a csv file named with query_treasury-ddo_year(all lowercase) format.
        '''
        treasury = response.meta.get('treasury_id')

        # header row for the file.
        heads = response.xpath('//table//tr[@class="popupheadingeKosh"]//td//text()').extract()

        # all other rows
        data_rows = response.xpath('//table//tr[contains(@class, "pope")]')

        if not data_rows:
            if DATASET_PAGE_ERROR_MSG in response.text:
                self.crawler.stats.inc_value('{}/dataset_not_avail_count'.format(treasury))
                self.logger.warning('No dataset found for {}'.format(response.url))
            return

        self.crawler.stats.inc_value('{}/dataset_count'.format(treasury))

        # get the filepath to save the records from meta
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

    def get_ddo_codes(self, treasury_id):
        '''
        collects and return ddo code for a treasury.
        '''
        try:
            ddo_dir_path = os.path.join(SPENDING_DATA_PATH, '{}_ddo_codes'.format(treasury_id))

            # NOTE: Ref-https://stackoverflow.com/a/44031522/3860168
            ddo_files = glob.glob('{}/*.csv'.format(ddo_dir_path))
            sorted_ddo_file_names = sorted(ddo_files, key=os.path.getmtime)
            ddo_file_path = sorted_ddo_file_names[-1]
        except (FileNotFoundError, IndexError):
            self.logger.error('No ddo code file exists for treasury: {}'.format(treasury_id))
        except Exception as err:
            self.logger.error(err)
        else:
            with open(ddo_file_path) as ddo_file:
                ddo_code_reader = csv.DictReader(ddo_file)

                for ddo in ddo_code_reader:
                    ddo_code = ddo['DDO Code']
                    yield ddo_code
