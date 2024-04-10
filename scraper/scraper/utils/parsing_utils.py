'''
Utility functions for parsers.
'''

import re


def clean_text(text):
    '''
    Removes extra whitespaces from in between and around the text.
    '''
    return re.sub(r'\s{2,}', ' ', text).strip()


def make_dataset_file_name(attrs):
    '''
    Creates a pattern for a filename for a dataset.
    '''
    filename = '{query}_{treasury}_{ddo}_{date}.csv'.format(**attrs)
    filename = re.sub(r',+', '', filename).replace(' ', '_').replace('/', '_')
    return filename
