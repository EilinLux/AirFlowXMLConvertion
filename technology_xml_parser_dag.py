

"""
author: Eilin Lux
email: eilinlux@gmail.com

"""


import datetime
from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule


#############################################
######## DAG VARIABLES ######################
#############################################


######## Local Market & Technologies  #####################################
lms = "es"
techs = "4G,5G,2G"

######## time variables #####################################
year = datetime.datetime.today().year
month = datetime.datetime.today().month
day = datetime.datetime.today().day
start = f"{year}{month}{day-1}" # TODO decide how often should it run 
end = f"{year}{month}{day}"



### TO BE CUSTUMIZED
#############################################
lm = "" 
environment_type = "" # ex. dev, prod, qa
release="" # r1, r2, etc.
goal =  "technology-xml-parser"
dag_name = '{}-{}-{}-{}'.format(goal,release, lm, environment_type)
cluster_name='{}-target-{}-{}-{}'.format(goal,year, month, day)
DAGS_FOLDER = ""


### [Required] Storage bucket specifications (where the code is saved)
#############################################
storage_bucket='bucket-{}-type-{}'.format(lm, environment_type) ##TOBEREPLACED
code_files = 'gs://{}/folder-name/{}'.format(storage_bucket, dag_name) ##TOBEREPLACED


# [Required] The Hadoop Compatible Filesystem (HCFS) URI of the main Python file to use as the driver. Must be a .py file.
run_main='{}/{}_main.py'.format(code_files, goal.replace("-","_")) 


# [NOT Required] Cloud Storage specifications (not mandatory)
# List of Python files to pass to the PySpark framework. Supported file types: .py, .egg, and .zip
# notice it will unpack without extracting the file in an omonimous folder, therefore be careful on the imports. 
# run_pyfiles=[
#             '{}/utils/transform.py'.format(code_files),
#             '{}/utils/read.py'.format(code_files),
#             '{}/utils/write.py'.format(code_files)
#             ]


### [Required] dataproc cluster specifications ##TOBEREPLACED
#############################################
subnetwork_uri='projects/project-{}-type-{}/regions/europe-west1/subnetworks/{}'.format(lm, environment_type,"-subnetwork-name" ) ##TOBEREPLACED
project_id = 'project-{}-type-{}'.format(lm,environment_type) ##TOBEREPLACED
service_account= 'sa@project-name.iam.gserviceaccount.com' ##TOBEREPLACED

spark_config_jar = 'gs://project-{}-type-{}/spark-xml/spark-xml_2.12-0.14.0.jar'.format(lm,environment_type) ##TOBEREPLACED
dataproc_pyspark_jars_list = ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar', spark_config_jar]



region='europe-west1'
zone='europe-west1-b'
image_version ='1.5.53-debian10'
master_machine_type='e2-standard-32'
master_disk_size = 800
worker_machine_type='e2-standard-32'
worker_disk_size = 800
num_workers=20
num_preemptible_workers = 0 # num-secondary-workers more info: https://cloud.google.com/dataproc/docs/concepts/compute/secondary-vms
idle_delete_ttl = 14400 # max-idle seconds equals to 14 h NEVER USE A STRING 


### Task ids specifications 
#############################################
create_task_id='create_cluster_{}'.format(dag_name.replace("-","_"))
run_task_id = 'run_pyspark_job_{}'.format(dag_name.replace("-","_"))
delete_task_id='delete_cluster_{}'.format(dag_name.replace("-","_"))

# Scheduler interval
#############################################
schedule_interval='0 0 5 * *'

## labeling 
#############################################
labels = {
        "cluster-goal":"dev", #  ex. dev, prod, qa
        "use-case":"technology", # TOBEREPLACED
        "cluster-owner":"eilinlux-gmail_com", # TOBEREPLACED
        "team-tag":"DataEngineer" # TOBEREPLACED
         }


#############################################
######## DAG DEFAULT ########################
#############################################
default_dag_args = {
    'start_date': datetime.datetime(2015, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': project_id,
    'year':year,
    'month':month,
    'day':day,
    'code_files':code_files
}


#############################################
######## DAG MODEL ##########################
#############################################

with models.DAG(
        dag_name,
        schedule_interval=schedule_interval,
        default_args=default_dag_args
        ) as dag:
    
    # override cluster creation to enable getway component
    class CustomDataprocClusterCreateOperator(dataproc_operator.DataprocClusterCreateOperator):
        def __init__(self, *args, **kwargs):
            super(CustomDataprocClusterCreateOperator, self).__init__(*args, **kwargs)
        def _build_cluster_data(self):
            cluster_data = super(CustomDataprocClusterCreateOperator, self)._build_cluster_data()
            cluster_data['config']['endpointConfig'] = {
            'enableHttpPortAccess': True
            }
            cluster_data['config']['softwareConfig']['optionalComponents'] = [ 'JUPYTER', 'ANACONDA' ]
            return cluster_data

    #Create features creation Dataproc cluster.
    create_cluster_task = CustomDataprocClusterCreateOperator(
        task_id=create_task_id,
        project_id=project_id,
        cluster_name=cluster_name,
        storage_bucket=storage_bucket,
        region=region, 
        zone=zone,
        service_account=service_account,
        subnetwork_uri=subnetwork_uri,
        labels=labels, 
        tags=['allow-internal-dataproc-proda', 'allow-ssh-from-management-zone','allow-ssh-from-net-to-bastion'],
        master_machine_type=master_machine_type,
        # master_disk_type='pd-standard',
        master_disk_size=master_disk_size,
        num_workers=num_workers,
        worker_machine_type=worker_machine_type,
        # worker_disk_type='pd-standard',
        worker_disk_size=worker_disk_size,
        num_preemptible_workers=num_preemptible_workers,
        image_version=image_version,
        idle_delete_ttl=idle_delete_ttl,
        internal_ip_only=True,
        metadata= {'enable-oslogin': 'true'}, 
        #  [('enable-oslogin', 'true'),
        # ('PIP_PACKAGES','google-cloud')
        # ], 
        # notice migrating to airFlow 1.0 to 2.0 need to change {'enable-oslogin': 'true'}, https://stackoverflow.com/questions/70423687/facing-issue-with-dataproccreateclusteroperator-airflow-2-0
        properties={
            'core:fs.gs.implicit.dir.repair.enable':'false', 
            'core:fs.gs.status.parallel.enable':'true',
            'dataproc:dataproc.logging.stackdriver.job.driver.enable':'true', 
            'yarn:yarn.nodemanager.resource.memory-mb':'118000', 'yarn:yarn.nodemanager.resource.cpu-vcores':'30',
            'yarn:yarn.nodemanager.pmem-check-enabled':'false', 'yarn:yarn.nodemanager.vmem-check-enabled':'false', 
            'dataproc:am.primary_only':'true', 
            'spark:spark.yarn.am.cores':'5', 'spark:spark.yarn.am.memory':'40g', # if memory error increase here
            'spark:spark.jars' : spark_config_jar,
            }
        )
      
    run_job_task = dataproc_operator.DataProcPySparkOperator(
        task_id=run_task_id,
        main=run_main,
        # pyfiles=run_pyfiles,
        cluster_name=cluster_name,
        region=region,
        arguments=[ 
        '--StartDate', str(start),
        '--EndDate',  str(end),
        '--lm', str(lms),
        '--techs',  str(techs),        
         ],
        dataproc_pyspark_jars = dataproc_pyspark_jars_list
    )

    #Delete Cloud Dataproc cluster.
    delete_cluster_task = dataproc_operator.DataprocClusterDeleteOperator(
       task_id=delete_task_id,
       cluster_name=cluster_name,
       region=region, 
       trigger_rule=trigger_rule.TriggerRule.ALL_DONE
    )

    # Define DAG dependencies.
    create_cluster_task >> run_job_task >> delete_cluster_task 

      