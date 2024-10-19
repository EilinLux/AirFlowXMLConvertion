"""
Author: Eilin Lux
Email: eilinlux@gmail.com
Description: This Airflow DAG processes XML files from Google Cloud Storage (GCS),
             flattens them based on technology (2G, 3G, 4G, 5G), and saves 
             the processed data as Parquet files back into GCS using a PySpark job 
             running on a Dataproc cluster. Configuration is loaded from airflow.cfg.
"""

import datetime
from airflow import models
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.utils import trigger_rule
from airflow.configuration import conf

##########################################################
# Get configuration from airflow.cfg and Airflow Variables
##########################################################

PROJECT_ID = Variable.get("project_id")
REGION = Variable.get("region")
LOCAL_MARKETS = Variable.get("local_markets")
ENVIRONMENT = Variable.get("environment_type")
TECHNOLOGIES = Variable.get("technologies")
GOAL = Variable.get("goal")
SCHEDULE_INTERVAL = Variable.get("schedule_interval")
LABELS = Variable.get("labels")
DEFAULT_DAG_ARGS = conf.getsection('DAG_DEFAULT')
DATAPROC_PYSPARK_JARS = Variable.get("dataproc_pyspark_jars")

##########################################################
# Dates for processing (defaults to yesterday)
##########################################################

YEAR = datetime.datetime.today().year
MONTH = datetime.datetime.today().month
DAY = datetime.datetime.today().day
START_DATE = f"{YEAR}{MONTH}{DAY-1}"
END_DATE = f"{YEAR}{MONTH}{DAY}"

##########################################################
# --- Construct DAG and cluster names ---
##########################################################

DAG_NAME = f"{GOAL}-{LOCAL_MARKETS}-{ENVIRONMENT}"
CLUSTER_NAME = f"{GOAL}-target-{YEAR}-{MONTH}-{DAY}"



##########################################################
# --- Storage bucket where code is stored ---
##########################################################

STORAGE_BUCKET = f"bucket-{LOCAL_MARKETS}-type-{ENVIRONMENT}"
CODE_FILES_FOLDER = f"gs://{STORAGE_BUCKET}/folder-name/{DAG_NAME}"
MAIN_PYTHON_FILE = f"{CODE_FILES_FOLDER}/{GOAL.replace('-','_')}_main.py"

##########################################################
# --- Task IDs ---
##########################################################
CREATE_CLUSTER_TASK_ID = f"create_cluster_{DAG_NAME.replace('-','_')}"
RUN_PYSPARK_JOB_TASK_ID = f"run_pyspark_job_{DAG_NAME.replace('-','_')}"
DELETE_CLUSTER_TASK_ID = f"delete_cluster_{DAG_NAME.replace('-','_')}"


# ---------------------------------------------
# --- DAG Definition ---
# ---------------------------------------------
with models.DAG(DAG_NAME, schedule_interval=SCHEDULE_INTERVAL, default_args=DEFAULT_DAG_ARGS) as dag:
    # Create Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id=CREATE_CLUSTER_TASK_ID,
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        storage_bucket=STORAGE_BUCKET,
        labels=LABELS,
        tags=[
            "allow-internal-dataproc-proda",
            "allow-ssh-from-management-zone",
            "allow-ssh-from-net-to-bastion",
        ],

        master_machine_type = Variable.get("master_machine_type")
        master_disk_size = int(Variable.get("master_disk_size"))  # Convert to integer
        num_workers = int(Variable.get("num_workers"))  # Convert to integer
        worker_machine_type = Variable.get("worker_machine_type")
        worker_disk_size = int(Variable.get("worker_disk_size"))  # Convert to integer
        num_preemptible_workers = int(Variable.get("num_preemptible_workers"))  # Convert to integer
        image_version = Variable.get("image_version")
        idle_delete_ttl = int(Variable.get("idle_delete_ttl"))  # Convert to integer


        internal_ip_only=True,
        metadata={"enable-oslogin": "true"},
        properties={
            "core:fs.gs.implicit.dir.repair.enable": "false",
            "core:fs.gs.status.parallel.enable": "true",
            "dataproc:dataproc.logging.stackdriver.job.driver.enable": "true",
            "yarn:yarn.nodemanager.resource.memory-mb": "118000",
            "yarn:yarn.nodemanager.resource.cpu-vcores": "30",
            "yarn:yarn.nodemanager.pmem-check-enabled": "false",
            "yarn:yarn.nodemanager.vmem-check-enabled": "false",
            "dataproc:am.primary_only": "true",
            "spark:spark.yarn.am.cores": "5",
            "spark:spark.yarn.am.memory": "40g",
            "spark:spark.jars": Variable.get("spark_config_jar"),
        },
    )

    # Define PySpark job
    pyspark_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": MAIN_PYTHON_FILE,
            "jar_file_uris": Variable.get("dataproc_pyspark_jars"),
            "args": [
                "--StartDate",
                START_DATE,
                "--EndDate",
                END_DATE,
                "--lm",
                LOCAL_MARKETS,
            ],
        },
    }

    # Submit PySpark job
    run_job = DataprocSubmitJobOperator(
        task_id=RUN_PYSPARK_JOB_TASK_ID,
        job=pyspark_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Delete Dataproc cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id=DELETE_CLUSTER_TASK_ID,
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
    )

    # Define DAG dependencies
    (create_cluster >> run_job >> delete_cluster)