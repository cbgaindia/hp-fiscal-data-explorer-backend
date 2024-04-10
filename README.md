# Backend for Himachal Pradesh Fiscal Data explorer

[![License: AGPLv3.0](https://img.shields.io/badge/License-MIT-lightgrey.svg)](https://github.com/CivicDataLab/hp-fiscal-data-explorer/blob/master/LICENSE)

Himachal Pradesh Fiscal Data explorer for [Open Budgets India](https://openbudgetsindia.org/) Platform.

### Setup Instructions for the Scraper

#### To setup the env, type the following commands in that order:

- `git clone https://github.com/CivicDataLab/hp-fiscal-data-explorer.git`
- `pip install pipenv`
- `cd scraper`
- `cp  scraper/settings/settings.py scraper/settings/local.py`
- `pipenv install --three --ignore-pipfile`
- `cp scraper/settings/settings.py scraper/settings/local.py`
- Edit the `scraper/settings/local.py` with setting values you want to keep like `DOWNLOAD_DELAY`, `CONCURRENT_REQUESTS` and custom settings like `DATASETS_PATH` etc.
- create a `datasets` directory at the path specified by `DATASET_PATH` variable. e.g. `hp-fiscal-data-explorer/scraper/datasets` if you didn't update the default value of the variable `DATASET_PATH`.

#### To Activate the environment once setup.

- `. $(pipenv --venv)/bin/activate`

#### Run the scraper for collecting ddo codes.
- `scrapy crawl ddo_collector`

#### Run the scraper for collecting datasets.
For Treasury Expenditure.
- `scrapy crawl treasury_expenditures -a start=20190501 -a end=20190531`

For Treasury Receipts.
- `scrapy crawl treasury_receipts -a start=20190501 -a end=20190531`

For Budget Expenditure.
- `scrapy crawl budget_expenditures -a date=20190531`

NOTES:
- the arguments `start` and `end` specifies the date range for datasets. The date format is `yyyymmdd`.
- the datasets will be `CSV` files with name in the format: `treasury_expenditures_<treasury>_<ddo>_<timestamp>.csv` for expenditures.
