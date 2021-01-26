import shutil
import tempfile
from logging import getLogger
from pathlib import Path
from typing import Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from common import DATAPUNT_ENVIRONMENT, MessageOperator, default_args, slack_webhook_token
from http_fetch_operator import HttpFetchOperator

FileStem = str
UrlPath = str

DAG_ID = "bbga"
TMP_DIR_BASE = Path("/tmp")
VARS = Variable.get(DAG_ID, deserialize_json=True)
data_endpoints: Dict[FileStem, UrlPath] = VARS["data_endpoints"]

logger = getLogger(__name__)

with DAG(dag_id=DAG_ID, default_args=default_args) as dag:
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {DAG_ID} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    mk_tmp_dir = PythonOperator(
        # The resulting tmp dir is returned via XCom using the task_id
        task_id="mk_tmp_dir",
        python_callable=tempfile.mkdtemp,
        op_kwargs=dict(prefix="bbga_", dir=TMP_DIR_BASE),
    )

    download_data = [
        HttpFetchOperator(
            task_id=f"download_{file_stem}",
            endpoint=url_path,
            http_conn_id="api_data_amsterdam_conn_id",
            tmp_file=f"{file_stem}.csv",
            xcom_tmp_dir_task_ids="mk_tmp_dir",
        )
        for file_stem, url_path in data_endpoints.items()
    ]

    def _rm_tmp_dir(**kwargs) -> None:
        tmp_dir = kwargs["ti"].xcom_pull(task_ids="mk_tmp_dir")
        logger.info("Removing tmp dir: '%s'.", tmp_dir)
        shutil.rmtree(tmp_dir)

    rm_tmp_dir = PythonOperator(
        task_id="rm_tmp_dir",
        python_callable=_rm_tmp_dir,
        provide_context=True,  # Ensure we can use XCom to retrieve the previously created tmp dir.
    )

    slack_at_start >> mk_tmp_dir >> download_data >> rm_tmp_dir
