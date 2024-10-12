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
Main ingestion entrypoint to run OM workflows
代码是 OpenMetadata 系统的主要数据摄取（ingestion）入口点，用于执行不同类型的工作流。
它的设计目的是在不同的环境中（例如 Airflow 的 KubernetesPodOperator）运行摄取任务
"""
import os

import yaml

from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    PipelineType,
)
from metadata.generated.schema.metadataIngestion.workflow import LogLevels
from metadata.utils.logger import set_loggers_level
from metadata.workflow.data_quality import TestSuiteWorkflow
from metadata.workflow.metadata import MetadataWorkflow
from metadata.workflow.profiler import ProfilerWorkflow
from metadata.workflow.usage import UsageWorkflow

"""
定义了 WORKFLOW_MAP，这是一个字典，键是 PipelineType 的值，值是对应的工作流类。
这些工作流类用于处理不同的摄取任务，例如元数据摄取、使用情况数据摄取、数据血缘追踪、数据质量测试等。
"""
WORKFLOW_MAP = {
    PipelineType.metadata.value: MetadataWorkflow,
    PipelineType.usage.value: UsageWorkflow,
    PipelineType.lineage.value: MetadataWorkflow,
    PipelineType.profiler.value: ProfilerWorkflow,
    PipelineType.TestSuite.value: TestSuiteWorkflow,
    PipelineType.elasticSearchReindex.value: MetadataWorkflow,
    PipelineType.dbt.value: MetadataWorkflow,
}


def main():
    """
    Ingestion entrypoint. Get the right Workflow class
    and execute the ingestion.

    This image is expected to be used and run in environments
    such as Airflow's KubernetesPodOperator:

    ```
    config = '''
        source:
          type: ...
          serviceName: ...
          serviceConnection:
            ...
          sourceConfig:
            ...
        sink:
          ...
        workflowConfig:
          ...
    '''

    KubernetesPodOperator(
        task_id="ingest",
        name="ingest",
        cmds=["python", "main.py"],
        image="openmetadata/ingestion-base:0.13.2",
        namespace='default',
        env_vars={"config": config, "pipelineType": "metadata"},
        dag=dag,
    )
    ```

    Note how we are expecting the env variables to be sent, with the `config` being the str
    representation of the ingestion YAML.

    We will also set the `pipelineRunId` value if it comes from the environment.
    """
    # 获取环境变量
    # DockerOperator expects an env var called config
    config = os.getenv("config")
    if not config:
        raise RuntimeError(
            "Missing environment variable `config`. This is needed to configure the Workflow."
        )

    # 获取工作流类
    pipeline_type = os.getenv("pipelineType")
    if not pipeline_type:
        raise RuntimeError(
            "Missing environment variable `pipelineType`. This is needed to load the Workflow class."
        )

    pipeline_run_id = os.getenv("pipelineRunId")

    # 加载工作流配置
    workflow_class = WORKFLOW_MAP.get(pipeline_type)
    if workflow_class is None:
        raise ValueError(f"Missing workflow_class loaded from {pipeline_type}")

    # Load the config string representation
    workflow_config = yaml.safe_load(config)
    if pipeline_run_id:
        workflow_config["pipelineRunId"] = pipeline_run_id

    # 设置日志级别
    logger_level = workflow_config.get("workflowConfig", {}).get("loggerLevel")
    set_loggers_level(logger_level or LogLevels.INFO.value)

    # 创建并执行工作流
    """
    使用 workflow_class.create(workflow_config) 来创建对应的工作流实例。
    调用 workflow.execute() 方法执行工作流。
    调用 workflow.raise_from_status() 检查工作流的执行状态，如果失败会抛出异常。
    调用 workflow.print_status() 输出工作流的状态。
    调用 workflow.stop() 停止工作流，进行清理操作。
    """
    workflow = workflow_class.create(workflow_config)
    workflow.execute()
    workflow.raise_from_status()
    workflow.print_status()
    workflow.stop()


if __name__ == "__main__":
    main()
