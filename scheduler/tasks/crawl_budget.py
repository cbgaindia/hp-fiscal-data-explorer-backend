'''
Crawler DAG definition.
'''

from datetime import datetime
from os import path
from string import Template

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

PROJECT_PATH = path.abspath(path.join(path.dirname(__file__), '../../scraper'))
BUDGET_DATA_PATH = path.abspath(path.join(PROJECT_PATH, 'budget_data'))

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime.today().replace(day=1),
    'concurrency': 1,
    # since scrapy crawlers already try 3 times at their end if there's network glitch or something
    # if there's some other issue then we should not anyway overwhelm the site by continuously
    # hitting.
    'retries': 0
}

with DAG('crawl_budget',
         default_args=DEFAULT_ARGS,
         schedule_interval='30 15 * * *',  # the timezone is UTC here.
         catchup=False
        ) as dag:

    CREATE_DIR = BashOperator(
        task_id='create_datasets_dir',
        bash_command="""
            if [ ! -d {path} ]; then mkdir -p {path}/expenditures {path}/receipts; fi
        """.format(path=BUDGET_DATA_PATH)
    )

    # Ref: https://airflow.apache.org/macros.html for the jinja variables used below.
    EXPENDITURE_CRAWL_COMMAND = Template("""
        cd $project_path && scrapy crawl budget_expenditures -a date={{ ds_nodash }}
    """)

    EXPENDITURE_CRAWL_TASK = BashOperator(
        task_id='crawl_budget_expenditure',
        bash_command=EXPENDITURE_CRAWL_COMMAND.substitute(project_path=PROJECT_PATH),
    )

    RECEIPTS_CRAWL_COMMAND = Template("""
        cd $project_path && scrapy crawl budget_receipts -a date={{ execution_date.strftime('%d/%m/%Y') }}
    """)

    RECEIPTS_CRAWL_TASK = BashOperator(
        task_id='crawl_budget_receipts',
        bash_command=RECEIPTS_CRAWL_COMMAND.substitute(project_path=PROJECT_PATH),
    )

CREATE_DIR.set_downstream(EXPENDITURE_CRAWL_TASK)
CREATE_DIR.set_downstream(RECEIPTS_CRAWL_TASK)
