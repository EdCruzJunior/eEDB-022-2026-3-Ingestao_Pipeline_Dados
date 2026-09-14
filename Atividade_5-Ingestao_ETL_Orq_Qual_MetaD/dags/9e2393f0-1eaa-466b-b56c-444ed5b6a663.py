"""
This file has been generated from dag_runner.j2
"""
from airflow import DAG
from openmetadata_managed_apis.workflows import workflow_factory

workflow = workflow_factory.WorkflowFactory.create("/opt/airflow/dags/generated_configs/9e2393f0-1eaa-466b-b56c-444ed5b6a663.json")
workflow.generate_dag(globals())
dag = workflow.get_dag()