import collections
import logging
from datetime import timedelta
import os

from airflow import DAG
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable, BaseOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from datetime import datetime
import boto3
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta, MO, TU
from airflow.operators.dummy_operator import DummyOperator
import re

AIRFLOW_EFS = Variable.get("AIRFLOW_EFS", deserialize_json=False, default_var=os.environ["AIRFLOW_HOME"])
PIPELINE_NAME = 'pan-bbc-churn-prediction-pipeline'
SLACK_CONN_ID = 'slack'
# Select schema per environment...
ENV = Variable.get("ENV", deserialize_json=False, default_var="TEST")
ENV_SCHEMAS = {
    'INT': 'sandbox',
    'TEST': 'central_insights_sandbox',
    'LIVE': 'central_insights'
}
#SCHEMA = ENV_SCHEMAS[ENV]
SCHEMA="central_insights_sandbox"
PROJECT_NAME="pan-bbc-churn-predictions"
PROJ_S3_BUCKET = f"{ENV}-insights-pan-bbc-churn-predictions".lower()
base_path = f"s3://{PROJ_S3_BUCKET}"
s3_path_input_training=f"{base_path}/data/input/training"
s3_path_input_score=f"{base_path}/data/input/score"
s3_path_current_propensity_scores=f"{base_path}/data/output/loyalty-propensity-scores"

class MyPostgresOperator(BaseOperator):
    """
    Custom postgres operator to get around templated variables inside a sql template
    Must specify templated variables as <>
    """

    template_fields = ('sql', 'parameters_dict',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql, parameters_dict=None,
            postgres_conn_id='postgres_default', autocommit=False,
            parameters=None,
            database=None,
            *args, **kwargs):
        super(MyPostgresOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.parameters_dict = parameters_dict
        self.params = self.parameters_dict

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        # Render variables into sql
        logging.info(f"params = {self.params}")
        logging.info(f"params dict = {self.parameters_dict}")
        for param, param_value in self.parameters_dict.items():
            param_name = f"<params.{param}>"
            logging.info(f"Replacing {param_name}={param_value}")
            self.sql = self.sql.replace(param_name, param_value)
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
        for output in self.hook.conn.notices:
            self.log.info(output)

def get_credentials_for_s3(role_to_assume=None, **kwargs):
    """
    Returns a dict with
    AccessKeyId
    SecretAccessKey
    SessionToken
    """
    sts = boto3.client('sts')

    if role_to_assume and len(role_to_assume) > 1:
        arn = role_to_assume

    else:
        current_role = sts.get_caller_identity()['Arn']
        logging.info(f"Current role {current_role}")

        regex = r"::(\d*):(role|assumed-role)\/([^\/]*)\/?.*$"
        matches = re.findall(regex, current_role)[0]
        account_number = matches[0]
        role_name = matches[2]

        arn = f"arn:aws:iam::{account_number}:role/{role_name}"

    logging.info(f"Getting credentials for role: {arn}")
    resp = sts.assume_role(
        RoleArn=arn,
        RoleSessionName='airflow'
    )

    if 'ti' in kwargs:
        ti = kwargs['ti']
        ti.xcom_push(key='AccessKeyId', value=resp['Credentials']['AccessKeyId'])
        ti.xcom_push(key='SecretAccessKey', value=resp['Credentials']['SecretAccessKey'])
        ti.xcom_push(key='SessionToken', value=resp['Credentials']['SessionToken'])
    logging.info(resp['Credentials'])
    logging.info('test log')
    return resp['Credentials']

def copy_s3_to_s3(source_bucket, source_folder, target_bucket, target_folder, role_to_assume=None,
                  clear_target_folder=False, single_object=False):
    """ Copies files from one s3 location to another, using an assumed role if provided """

    # Assume the role we need
    if role_to_assume and len(role_to_assume) > 1:
        logging.info(f"Assuming role for s3 copy: {role_to_assume}")
        sts_client = boto3.client('sts')

        assumed_role_creds = sts_client.assume_role(
            RoleArn=role_to_assume,
            RoleSessionName="AssumeRoleSession1"
        )['Credentials']

        s3 = boto3.resource(
            's3',
            aws_access_key_id=assumed_role_creds['AccessKeyId'],
            aws_secret_access_key=assumed_role_creds['SecretAccessKey'],
            aws_session_token=assumed_role_creds['SessionToken'],
        )
    else:
        s3 = boto3.resource('s3')

    logging.info(f"Copying (recursively) s3://{source_bucket}/{source_folder} to s3://{target_bucket}/{target_folder}")

    _target_bucket = s3.Bucket(target_bucket)

    # Clear out target folder
    if clear_target_folder:
        logging.info(f"Clearing out target folder s3://{target_bucket}/{target_folder}")
        _target_bucket.objects.filter(Prefix=os.path.join(target_folder, '')).delete()

    if single_object:
        # Just copy one object
        logging.info(f"Just copying one object from s3://{source_bucket}/{source_folder} to s3://{target_bucket}/{target_folder}")
        copy_source = {
            'Bucket': source_bucket,
            'Key': source_folder
        }
        fname = os.path.basename(source_folder)
        _target_bucket.copy(copy_source, os.path.join(target_folder, fname))
    else:
        # Need to implement own recursive copy
        for obj in s3.Bucket(source_bucket).objects.filter(Prefix=source_folder):
            copy_source = {
                'Bucket': source_bucket,
                'Key': obj.key
            }
            target_key = os.path.join(target_folder, obj.key.replace(source_folder, '').lstrip('/'))
            logging.info(f"Copying s3://{source_bucket}/{obj.key} to s3://{target_bucket}/{target_key}")
            _target_bucket.copy(copy_source, target_key)
    return f"Segment copied to s3://{target_bucket}/{target_folder}"

def task_slack_alert(context, fail=True, xcoms_to_include=None, **kwargs):
    """ Sends message to a slack channel, switch between fail / success """
    if context is None:
        context = kwargs
        ti = kwargs['ti']
    else:
        ti = context.get('task_instance')

    status_msg = ':red_circle: *Task Failure!!!* :feelsbadman: @here'
    if not fail:
        status_msg = ':heavy_check_mark: *Task Success* :borat_says_yes:'

    msg = 'pan_bbc_churn_prediction_pipeline'
    xcoms = ti.xcom_pull(key=None, task_ids=xcoms_to_include)
    if isinstance(xcoms, collections.Iterable) and not isinstance(xcoms, str):
        for res in xcoms:
                msg += str(res) + '\n'
    elif xcoms:
        msg = xcoms

    if fail:
        msg += f"""
        *Encountered an error:*
        {type(context['exception'])}:
        {context['exception']}
        """

    return SlackWebhookOperator(
        task_id='slack_failed_alert',
        http_conn_id=SLACK_CONN_ID,
        link_names=True,
        message=f"""
    {status_msg}
    *Task*: {context.get('task_instance').task_id} 
    *Dag*: :tada: *{PIPELINE_NAME}* :tada:
    *Execution Time*: {context.get('execution_date')}  
    *Message*:  {msg} 
    """).execute(context=context)

############
# FUNCTIONS
############


class RunDataCheckOperator(BaseOperator):
    """
    Extension of Postgres Operator to do checks on data
    Checks should return no rows if passing
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql,
            postgres_conn_id='postgres_default', autocommit=False,
            parameters=None,
            database=None,
            check_name=None,
            raise_error=False,
            raise_warning=True,
            *args, **kwargs):
        super(RunDataCheckOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.check_name = check_name
        self.raise_error = raise_error
        self.raise_warning = raise_warning

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        df = self.hook.get_pandas_df(self.sql, parameters=self.parameters)
        df_string = df.to_string(index=False, header=False)
        for output in self.hook.conn.notices:
            self.log.info(output)
        msg = None
        if len(df) > 0:
            logging.info("Something is wrong with the data, checks return zero rows if everything is ok")
            if self.raise_error:
                raise RuntimeError(f"Check *{self.check_name}* has failed for the following dates:\n```\n{df_string}\n```")
            elif self.raise_warning:
                msg = f"\n@here - :red-cross: Check *{self.check_name}* has failed for the following dates: :red-cross:\n```\n{df_string}\n```"
            else:
                msg = f"\n*{self.check_name}*\n```\n{df_string}\n```"
        else:
            msg = f"\nCheck *{self.check_name}* passed :tick:"
        return msg

############
# DAG
############

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':'2019-12-05',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
 #   'on_failure_callback': task_slack_alert
}

logging.info(f"Template searchpath: {AIRFLOW_EFS}/plugins/{PIPELINE_NAME}/src/sql/")
dag = DAG(
    'pan-bbc-churn-pipeline-final',
    default_args=default_args,
    template_searchpath=[f"{AIRFLOW_EFS}/plugins/{PROJECT_NAME}"],
    description='Generate user churn probability predictions for iPlayer users',
    schedule_interval='0 12 * * SUN',
    max_active_runs=1,
    on_failure_callback=task_slack_alert
)

# join_session_and_schedule = PostgresOperator(
#     task_id='join_session_and_schedule',
#     postgres_conn_id='scv_redshift',
#     autocommit=False,
#     sql="join_session_and_schedule.sql",
#     params={
#         'SCHEMANAME': SCHEMA
#     },
#     dag=dag
# )
#

churn_target = PostgresOperator(
    task_id='churn_target',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/0.1-churn-target.sql",
    params={
        'SCHEMANAME': SCHEMA
    },
    dag=dag
)

churn_sounds = PostgresOperator(
    task_id='churn_sounds',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/0.2-churn-sounds-only.sql",
    params={
        'SCHEMANAME': SCHEMA
    },
    dag=dag
)

churn_affinity = PostgresOperator(
    task_id='churn_affinity',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/0.3-churn-affinity.sql",
    params={
        'SCHEMANAME': SCHEMA
    },
    dag=dag
)

churn_lastweek = PostgresOperator(
    task_id='churn_lastweek',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/0.4-churn-lastweek.sql",
    params={
        'SCHEMANAME': SCHEMA
    },
    dag=dag
)

churn_demos = PostgresOperator(
    task_id='churn_demos',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/0.5.churn-demos.sql",
    params={
        'SCHEMANAME': SCHEMA
    },
    dag=dag
)

churn_freq_segs = PostgresOperator(
    task_id='churn_freq_segs',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/0.6.churn-freq-segs.sql",
    params={
        'SCHEMANAME': SCHEMA
    },
    dag=dag
)

churn_devices = PostgresOperator(
    task_id='churn_devices',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/0.7-churn-devices.sql",
    params={
        'SCHEMANAME': SCHEMA
    },
    dag=dag
)

churn_content = PostgresOperator(
    task_id='churn_content',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/0.8-churn-content.sql",
    params={
        'SCHEMANAME': SCHEMA
    },
    dag=dag
)

churn_marketing = PostgresOperator(
    task_id='churn_marketing',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/0.9-churn-marketing.sql",
    params={
        'SCHEMANAME': SCHEMA
    },
    dag=dag
)

churn_uas_follows = PostgresOperator(
    task_id='churn_uas_follows',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/0.10-churn-uas-follows.sql",
    params={
        'SCHEMANAME': SCHEMA
    },
    dag=dag
)

churn_time_and_day = PostgresOperator(
    task_id='churn_time_and_day',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/0.11-churn-timeandday.sql",
    params={
        'SCHEMANAME': SCHEMA
    },
    dag=dag
)


churn_iplayer_featureset = PostgresOperator(
    task_id='churn_iplayer_featureset',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/1.1-churn-iplayer-featureset.sql",
    params={
        'SCHEMANAME': SCHEMA
    },
    dag=dag
)
churn_sounds_featureset = PostgresOperator(
    task_id='churn_sounds_featureset',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/1.2-churn-sounds-featureset.sql",
    params={
        'SCHEMANAME': SCHEMA
    },
    dag=dag
)

unload_trainining_set = MyPostgresOperator(
    task_id='unload_trainining_set',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/unload_churn_train.sql",
    parameters_dict={
        'SCHEMANAME': SCHEMA,
        'S3_OUTPUT_PATH': f"{s3_path_input_training}/iplayer_training_set.csv",
        'AWS_ACCESS_KEY_ID': f"{{{{ ti.xcom_pull(task_ids='get_credentials_for_s3_task', key='AccessKeyId') }}}}",
        'AWS_SECRET_ACCESS_KEY': f"{{{{ ti.xcom_pull(task_ids='get_credentials_for_s3_task', key='SecretAccessKey') }}}}",
        'TOKEN': f"{{{{ ti.xcom_pull(task_ids='get_credentials_for_s3_task', key='SessionToken') }}}}"
    },
    dag=dag
)

unload_score_set = MyPostgresOperator(
    task_id='unload_score_set',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/unload_churn_score.sql",
    parameters_dict={
        'SCHEMANAME': SCHEMA,
        'S3_OUTPUT_PATH':f"{s3_path_input_score}/iplayer_churn_score_sample.csv",
         'AWS_ACCESS_KEY_ID': f"{{{{ ti.xcom_pull(task_ids='get_credentials_for_s3_task', key='AccessKeyId') }}}}",
        'AWS_SECRET_ACCESS_KEY': f"{{{{ ti.xcom_pull(task_ids='get_credentials_for_s3_task', key='SecretAccessKey') }}}}",
        'TOKEN': f"{{{{ ti.xcom_pull(task_ids='get_credentials_for_s3_task', key='SessionToken') }}}}"
    },
    dag=dag
)
command={
        'command': [
            '/usr/local/bin/run-pan-bbc-churn-predictions.sh',
        ]
    }
aws_job_submission = AWSBatchOperator(
    task_id='aws-batch-job-submission',
    job_name='airflow-job-submission-and-run-' +  datetime.today().strftime('%Y-%m-%d'),
    job_definition='int-Airflow-Pan-BBC-Churn-Classification',
    job_queue='first-run-job-queue',
    overrides=command,
    dag=dag)


## Copy shap values and propensity scores to historical storage

last_monday = date.today() + relativedelta(weekday=TU(-2))
historical_storage = 'historical-storage'
s3_target_location = f"{historical_storage}/week_start={last_monday}/"


'''
export AWS_DEFAULT_REGION=eu-west-1
ENV=${ENV:-int}

current_date=`python get_current_week_start.py`
echo ${current_date}

'''
last_monday = date.today() + relativedelta( weekday=MO( -1 ) )
s3_target_location = f"week_start={last_monday}"

source_propensity_scores_path="data/output/loyalty-propensity-scores/"
target_propensity_scores_path="historical-storage/" + s3_target_location + "/loyalty-propensity-scores/"
source_models_path="models/"
target_models_path="historical-storage/" + s3_target_location + "/models/"
source_shap_values_path="loyalty/iplayer/shap-values/"
target_shap_values_path="historical-storage/" + s3_target_location + "/shap-values"
source_logs_path="data/output/logs/"
target_logs_path="historical-storage/" + s3_target_location + "/logs"
source_charts_path="data/output/charts/"
target_charts_path="historical-storage/"+ s3_target_location +"/charts/"

copy_propensity_scores = PythonOperator(
    task_id="copy_propensity_scores",
    python_callable=copy_s3_to_s3,
    op_kwargs={
        'source_bucket': PROJ_S3_BUCKET,
        'source_folder': source_propensity_scores_path,
        'target_bucket': PROJ_S3_BUCKET,
        'target_folder': target_propensity_scores_path ,
        'single_object': True
    },
    dag=dag)

copy_models = PythonOperator(
    task_id="copy_models",
    python_callable=copy_s3_to_s3,
    op_kwargs={
        'source_bucket': PROJ_S3_BUCKET,
        'source_folder': source_models_path,
        'target_bucket': PROJ_S3_BUCKET,
        'target_folder': target_models_path,
        'single_object': True
    },
    dag=dag)

copy_shap_values = PythonOperator(
    task_id="copy_shap_values",
    python_callable=copy_s3_to_s3,
    op_kwargs={
        'source_bucket': PROJ_S3_BUCKET,
        'source_folder': source_shap_values_path,
        'target_bucket': PROJ_S3_BUCKET,
        'target_folder': target_shap_values_path
    },
    dag=dag)

copy_logs = PythonOperator(
    task_id="copy_logs",
    python_callable=copy_s3_to_s3,
    op_kwargs={
        'source_bucket': PROJ_S3_BUCKET,
        'source_folder': source_logs_path,
        'target_bucket': PROJ_S3_BUCKET,
        'target_folder': target_logs_path
    },
    dag=dag)

copy_charts = PythonOperator(
    task_id="copy_charts",
    python_callable=copy_s3_to_s3,
    op_kwargs={
        'source_bucket': PROJ_S3_BUCKET,
        'source_folder': source_charts_path,
        'target_bucket': PROJ_S3_BUCKET,
        'target_folder': target_charts_path,
        'single_object': True
    },
    dag=dag)

copy_historic_data_to_overall_storage=[copy_propensity_scores, copy_models, copy_shap_values, copy_logs, copy_charts]

get_credentials_for_s3_task = PythonOperator(
    task_id='get_credentials_for_s3_task',
    python_callable=get_credentials_for_s3,
    provide_context=True,
    dag=dag )

append_propensity_scores=MyPostgresOperator(
    task_id='append_propensity_scores_test',
    postgres_conn_id='scv_redshift',
    autocommit=False,
    sql="SQL/pipeline/append_propensity_scores_to_redshift_table.sql",
    parameters_dict={
        'SCHEMANAME': SCHEMA,
        'S3_PATH': f"{s3_path_current_propensity_scores}/model_scores.csv",
        'TABLENAME':'iplayer_churn_propensity_scores',
        'AWS_ACCESS_KEY_ID': f"{{{{ ti.xcom_pull(task_ids='get_credentials_for_s3_task', key='AccessKeyId') }}}}",
        'AWS_SECRET_ACCESS_KEY': f"{{{{ ti.xcom_pull(task_ids='get_credentials_for_s3_task', key='SecretAccessKey') }}}}",
        'TOKEN': f"{{{{ ti.xcom_pull(task_ids='get_credentials_for_s3_task', key='SessionToken') }}}}"
    },
    dag=dag
)

# slack_notify_success = PythonOperator(
#     task_id='slack_notify_success',
#     python_callable=task_slack_alert,
#     provide_context=True,
#     op_kwargs={
#         'context': None,
#         'fail': False,
#     },
#     trigger_rule='none_failed',
#     dag=dag)

dummy_task=DummyOperator(
    task_id="dummy_task",
    dag=dag
)

#Set order
churn_target >> churn_sounds >> churn_affinity >> churn_lastweek >> churn_demos >> churn_freq_segs >> \
    churn_devices >> churn_content >> churn_marketing >> churn_uas_follows >> churn_time_and_day >> churn_iplayer_featureset >> churn_sounds_featureset >> \
get_credentials_for_s3_task >> [unload_trainining_set, unload_score_set] >> aws_job_submission >>  copy_historic_data_to_overall_storage >> dummy_task >> append_propensity_scores
