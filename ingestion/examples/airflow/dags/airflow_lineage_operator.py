#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
You can run this DAG from the default OM installation.

For this DAG to run properly we expected an OpenMetadata
Airflow connection named `openmetadata_conn_id`.

这段代码实现了一个名为 lineage_tutorial_operator 的 Airflow DAG (Directed Acyclic Graph)，
展示了如何在 Airflow 中使用 Bash 任务、模板化命令，以及如何集成 OpenMetadata 溯源操作。
它演示了如何通过 Airflow 任务生成时间信息、使用任务重试策略、并且通过 OpenMetadataLineageOperator 来记录数据流和溯源信息。
"""
from datetime import datetime
from textwrap import dedent

import requests

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

from airflow_provider_openmetadata.hooks.openmetadata import OpenMetadataHook

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
from airflow_provider_openmetadata.lineage.operator import OpenMetadataLineageOperator

OM_HOST_PORT = "http://localhost:8585/api"
OM_JWT = "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXBiEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKyNM-Tj93FDzq91jsyNmsQhyNv_fNr3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3ud-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg"
AIRFLOW_HOST_API_ROOT = "http://localhost:8080/api/v1/"
DEFAULT_OM_AIRFLOW_CONNECTION = "openmetadata_conn_id"
DEFAULT_AIRFLOW_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Basic YWRtaW46YWRtaW4=",
}

default_args = {
    "retries": 0,
}

# Create the default OpenMetadata Airflow Connection (if it does not exist)
"""
检查 OpenMetadata Airflow 连接
通过 Airflow 的 REST API 获取 OpenMetadata 的 Airflow 连接 openmetadata_conn_id。
如果连接不存在 (404)，则通过 POST 请求创建一个连接。
"""
res = requests.get(
    AIRFLOW_HOST_API_ROOT + f"connections/{DEFAULT_OM_AIRFLOW_CONNECTION}",
    headers=DEFAULT_AIRFLOW_HEADERS,
)
if res.status_code == 404:  # not found
    requests.post(
        AIRFLOW_HOST_API_ROOT + "connections",
        json={
            "connection_id": DEFAULT_OM_AIRFLOW_CONNECTION,
            "conn_type": "openmetadata",
            "host": "openmetadata-server",
            "schema": "http",
            "port": 8585,
            "password": OM_JWT,
        },
        headers=DEFAULT_AIRFLOW_HEADERS,
    )

elif res.status_code != 200:
    raise RuntimeError(f"Could not fetch {DEFAULT_OM_AIRFLOW_CONNECTION} connection")

# 定义 DAG
with DAG(
    "lineage_tutorial_operator",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=None,
    is_paused_upon_creation=True,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # BashOperator 任务    
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
        outlets={
            "tables": [
                "test-service-table-lineage.test-db.test-schema.lineage-test-outlet"
            ]
        },
    )

    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 1",
        retries=3,
        inlets={
            "tables": [
                "test-service-table-lineage.test-db.test-schema.lineage-test-inlet",
                "test-service-table-lineage.test-db.test-schema.lineage-test-inlet2",
            ]
        },
    )

    dag.doc_md = (
        __doc__  # providing that you have a docstring at the beginning of the DAG
    )
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    # 模板化 BashOperator
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id="templated",
        depends_on_past=False,
        bash_command=templated_command,
        params={"my_param": "Parameter I passed in"},
    )

    t1 >> [t2, t3]  # t1 执行完后，t2 和 t3 同时并行执行。

    # 用于记录数据溯源信息，并将其上传到 OpenMetadata 服务器。
    t4 = OpenMetadataLineageOperator(
        task_id="lineage_op",
        depends_on_past=False,
        server_config=OpenMetadataHook(DEFAULT_OM_AIRFLOW_CONNECTION).get_conn(),
        service_name="airflow_lineage_op_service",
        only_keep_dag_lineage=True,
    )

    [t1, t2, t3] >> t4   # t4 依赖于 t1、t2 和 t3，表示 t1、t2 和 t3 完成后，t4 才会执行。
