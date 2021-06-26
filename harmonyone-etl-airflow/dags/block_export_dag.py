from __future__ import print_function

from harmonyoneetl.build_export_dag import build_export_dag
from harmonyoneetl.variables import read_export_dag_vars

# When searching for DAGs, Airflow will only consider files where the string "airflow" and "DAG" both appear in the
# contents of the .py file.
DAG = build_export_dag(
    dag_id='mainnet',
    **read_export_dag_vars(
        provider_uri='https://rpc.s0.t.hmny.io',
        var_prefix='mainnet_',
        export_schedule_interval='0 12 * * *',
        export_start_date='2016-01-03',
        export_max_workers=4,
        export_batch_size=1,
    )
)
