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
OpenMetadata Airflow Lineage Backend example. Airflow provides a pluggable lineage backend that can
read a DAG's configured inlets and outlets to compose a lineage. With OpenMetadata we have a airflow lineage backend
to get all of the workflows in Airflow and also any lineage user's configured.
Please refer to https://docs.open-metadata.org/connectors/pipeline/airflow/lineage-backend on how to configure the lineage backend
with Airflow Scheduler
This is an example to demonstrate on how to configure a Airflow DAG's inlets and outlets

Airflow 提供了一个可插拔的溯源（lineage）后端，它可以读取 DAG 中配置的 inlets 和 outlets 来构建任务之间的溯源关系。
借助 OpenMetadata，我们可以使用 Airflow 溯源后端来获取 Airflow 中的所有工作流以及用户配置的任何溯源信息。
这是一个示例，演示如何为 Airflow DAG 配置 inlets 和 outlets。
"""


from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from metadata.generated.schema.entity.data.container import Container
from metadata.generated.schema.entity.data.table import Table
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity

# 定义默认参数：每个任务（Task）在 DAG 中都会继承这些默认值，除非在任务本身做了覆盖定义。这样可以避免为每个任务重复配置相同的参数。
default_args = { 
    "owner": "openmetadata_airflow_example",
    "depends_on_past": False,
    "email": ["user@company.com"],
    "execution_timeout": timedelta(minutes=5),
}

# DAG定义
@dag(
    default_args=default_args,
    dag_id="sample_lineage",
    description="OpenMetadata Airflow Lineage example DAG",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

# 任务定义
# 这三行代码会依次调用每个任务。
  虽然这里的任务没有真正的处理逻辑，但通过 inlets 和 outlets 的配置，Airflow 和 OpenMetadata 可以自动记录和显示数据流动的路径及任务之间的依赖关系。
def openmetadata_airflow_lineage_example():
    # 这个任务只用于展示溯源配置，没有具体的功能实现
    @task(
        inlets={
            "tables": [
                "sample_data.ecommerce_db.shopify.raw_order",  # 输入表
            ],
        },
        outlets={"tables": ["sample_data.ecommerce_db.shopify.fact_order"]},   # 输出表
    )
    def generate_data():
        pass

    @task(
        inlets=[
            OMEntity(entity=Container, fqn="s3_storage_sample.transactions", key="test")
        ],
        outlets=[
            OMEntity(
                entity=Table,
                fqn="sample_data.ecommerce_db.shopify.raw_order",
                key="test",
            )
        ],
    )
    def generate_data2():
        pass

    @task(
        inlets=[
            {
                "entity": "container",
                "fqn": "s3_storage_sample.departments",
                "key": "test",
            },
        ],
        outlets=[
            {
                "entity": "table",
                "fqn": "sample_data.ecommerce_db.shopify.raw_order",
                "key": "test",
            },
        ],
    )
    def generate_data3():
        pass

    generate_data()
    generate_data2()
    generate_data3()


openmetadata_airflow_lineage_example_dag = openmetadata_airflow_lineage_example()
