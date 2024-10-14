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
This DAG can be used directly in your Airflow instance after installing
the `openmetadata-ingestion[airflow-container]` package. Its purpose
is to connect to the underlying database, retrieve the information
and push it to OpenMetadata.

这段代码是一个 Airflow DAG（Directed Acyclic Graph）示例，
展示了如何使用 PythonOperator 从 Airflow 实例中提取元数据，并将其推送到 OpenMetadata（OM）中。
代码核心是通过配置文件 config 和 MetadataWorkflow 类实现元数据的收集和传输。
"""
from datetime import timedelta

import yaml
from airflow import DAG

try:
    from airflow.operators.python import PythonOperator
except ModuleNotFoundError:
    from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

from metadata.workflow.metadata import MetadataWorkflow

default_args = {
    "owner": "user_name",
    "email": ["username@org.com"],
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}



"""
1. 配置描述了数据源（source）和数据接收端（sink）的设置，以及 OpenMetadata 服务器的连接信息
2. 用函数定义数据摄取的工作流逻辑，函数中解析1的配置获取数据和环境
3. 创建工作流DAG，运行数据摄取函数，定义工作时间，时长，间隔等
"""

# config：这是一个 YAML 格式的配置字符串。该配置描述了数据源（source）和数据接收端（sink）的设置，以及 OpenMetadata 服务器的连接信息
config = """
source:
  type: airflow                # 定义数据源为 airflow
  serviceName: airflow_source
  serviceConnection:           # 连接配置信息（如主机地址和端口号）      
    config:
      type: Airflow
      hostPort: http://localhost:8080
      numberOfStatus: 10
      connection:
        type: Backend
  sourceConfig:
    config:
      type: PipelineMetadata
sink:
  type: metadata-rest
  config: {}
workflowConfig:
  loggerLevel: INFO
  openMetadataServerConfig:
    hostPort: http://openmetadata-server:8585/api
    authProvider: openmetadata
    securityConfig:
      jwtToken: "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXBiEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKyNM-Tj93FDzq91jsyNmsQhyNv_fNr3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3ud-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg"
"""

# 该函数定义了数据摄取的工作流逻辑
def metadata_ingestion_workflow():
    workflow_config = yaml.safe_load(config)  # 解析上面的 YAML 配置字符串为 Python 字典。
    workflow = MetadataWorkflow.create(workflow_config)   # 创建一个 MetadataWorkflow 实例，使用解析后的配置。
    workflow.execute()  # 执行工作流，开始从 Airflow 获取数据并推送到 OpenMetadata。
    workflow.raise_from_status()  # 如果工作流中有错误或任务失败，会抛出异常。
    workflow.print_status()  # 打印工作流的状态信息。
    workflow.stop()  # 停止工作流，释放资源。

# 创建 DAG
with DAG(
    "airflow_metadata_extraction",
    default_args=default_args,                 # 继承上面定义的默认参数。
    description="An example DAG which pushes Airflow data to OM",
    start_date=days_ago(1),           # 任务的开始时间为昨天。
    is_paused_upon_creation=True,              # DAG 创建后默认是暂停状态。
    schedule_interval="*/5 * * * *",         # 每 5 分钟运行一次
    catchup=False,       # 不会补齐过去没有运行的任务。
) as dag:        # 定义任务
    ingest_task = PythonOperator(
        task_id="ingest_using_recipe",
        python_callable=metadata_ingestion_workflow,      #使用 PythonOperator 运行前面定义的 metadata_ingestion_workflow() 函数
    )
