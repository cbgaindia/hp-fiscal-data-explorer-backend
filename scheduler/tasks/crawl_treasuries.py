'''
Crawler DAG definition.
'''

import glob
from datetime import datetime
from os import path
from string import Template

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator

PROJECT_PATH = path.abspath(path.join(path.dirname(__file__), '../../scraper'))
SPENDING_DATA_PATH = path.abspath(path.join(PROJECT_PATH, 'spending_data'))

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime.today().replace(day=1),
    'concurrency': 1,
    # since scrapy crawlers already try 3 times at their end if there's network glitch or something
    # if there's some other issue then we should not anyway overwhelm the site by continuously
    # hitting.
    'retries': 0
}

def branch_tasks(execution_date, **kwargs):  # pylint: disable=unused-argument
    '''
    Branch the tasks based on weekday.
    '''
    # check if the execution day is 'Friday'
    if(execution_date.weekday() == 3 or
       not glob.glob('{}/*_ddo_codes'.format(SPENDING_DATA_PATH))
       ):
        return ['crawl_ddo_codes', 'crawl_expenditure', 'crawl_receipts']

    return ['crawl_expenditure', 'crawl_receipts']


with DAG('crawl_treasuries',
         default_args=DEFAULT_ARGS,
         schedule_interval='30 10 * * *',  # the timezone is UTC here.
         catchup=False
        ) as dag:

    CREATE_DIR = BashOperator(
        task_id='create_datasets_dir',
        bash_command="""
            if [ ! -d {path} ]; then mkdir -p {path}/expenditures {path}/receipts; fi
        """.format(path=SPENDING_DATA_PATH)
    )

    CRAWL_DDO_CODES = BashOperator(
        task_id='crawl_ddo_codes',
        bash_command='cd {} && scrapy crawl ddo_collector'.format(PROJECT_PATH)
    )

    # Ref: https://airflow.apache.org/macros.html for the jinja variables used below.
    EXP_CRAWL_COMMAND = Template("""
        cd $project_path && scrapy crawl treasury_expenditures -a start={{ ds_nodash }} -a end={{ ds_nodash }}
    """)

    EXP_CRAWL_TASK = BashOperator(
        task_id='crawl_expenditure',
        bash_command=EXP_CRAWL_COMMAND.substitute(project_path=PROJECT_PATH),
        trigger_rule='none_failed'
    )

    REC_CRAWL_COMMAND = Template("""
        cd $project_path && scrapy crawl treasury_receipts -a start={{ ds_nodash }} -a end={{ ds_nodash }}
    """)

    REC_CRAWL_TASK = BashOperator(
        task_id='crawl_receipts',
        bash_command=REC_CRAWL_COMMAND.substitute(project_path=PROJECT_PATH),
        trigger_rule='none_failed'
    )

BRANCH_OP = BranchPythonOperator(
    task_id='branch_task',
    provide_context=True,
    python_callable=branch_tasks,
    dag=dag
)

CREATE_DIR.set_downstream(BRANCH_OP)
BRANCH_OP.set_downstream([CRAWL_DDO_CODES, EXP_CRAWL_TASK, REC_CRAWL_TASK])
CRAWL_DDO_CODES.set_downstream(EXP_CRAWL_TASK)
CRAWL_DDO_CODES.set_downstream(REC_CRAWL_TASK)
