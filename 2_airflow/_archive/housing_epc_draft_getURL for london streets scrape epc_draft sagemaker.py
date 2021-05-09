from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
import logging

# airflow operators
import airflow
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# airflow sagemaker operators
from airflow.contrib.operators.sagemaker_training_operator \
    import SageMakerTrainingOperator
from airflow.contrib.operators.sagemaker_tuning_operator \
    import SageMakerTuningOperator
from airflow.contrib.operators.sagemaker_transform_operator \
    import SageMakerTransformOperator
from airflow.contrib.hooks.aws_hook import AwsHook

# sagemaker sdk
import boto3
import sagemaker
from sagemaker.amazon.amazon_estimator import get_image_uri
from sagemaker.estimator import Estimator
from sagemaker.tuner import HyperparameterTuner

# airflow sagemaker configuration
from sagemaker.workflow.airflow import training_config
from sagemaker.workflow.airflow import tuning_config
from sagemaker.workflow.airflow import transform_config_from_estimator

#From Walter

from airflow.contrib.operators.sagemaker_training_operator import SageMakerTrainingOperator
# sagemaker sdk
import sagemaker
import boto3
from sagemaker.processing import Processor
# To define estimator
from sagemaker.estimator import Estimator



## ml workflow specific
#from pipeline import prepare, preprocess
#import config as cfg


log = logging.getLogger(__name__)


# =============================================================================
# 1. Set up the main configurations of the dag
# =============================================================================
# now = datetime.now() # current date and time
# date_time = now.strftime("%Y_%m_%d_%HH")
# print("date and time:",date_time)

default_args = {
    'start_date': datetime(2021, 3, 8),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'email_on_failure': True,
    'email_on_retry': False,
    'aws_conn_id': "AWS_default_DanielaAvramioti",
    'bucket_name': Variable.get("housing_epc_rating", deserialize_json=True)['bucket_name_output'],
    'postgres_conn_id': 'postgres_housing_epc_conn',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'db_name': Variable.get("housing_epc_rating", deserialize_json=True)['db_name']
}

##config = {}
##
##
##config["preprocess_data"] = {
##    "s3_in_url": "s3://ucl-msin0166-2021-london-housing-epc/output/dexters_epc_ratings.csv",
##    "s3_out_bucket": "ucl-msin0166-2021-london-housing-epc",  # replace
##    "s3_out_prefix": "preprocess/",
##    "delimiter": "\t"
##}


dag = DAG('housing_listings_epc_scraping_sagemaker',
          description='Web scraping pipeline scraping housing data and energy ratings and saving output to a postgreSQL db in RDS',
          schedule_interval='@monthly',
          catchup=False,
          default_args=default_args,
          max_active_runs=1)

# =============================================================================
# 2. Define different functions
# =============================================================================
#creating Sagemaker Hook:

class SageMakerHook(AwsHook):
    """
    Interact with Amazon SageMaker.
    """
    non_terminal_states = {'InProgress', 'Stopping'}
    endpoint_non_terminal_states = {'Creating', 'Updating', 'SystemUpdating',
                                    'RollingBack', 'Deleting'}
    failed_states = {'Failed'}

    def __init__(self,
                 *args, **kwargs):
        super(SageMakerHook, self).__init__(*args, **kwargs)
        self.s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

    def tar_and_s3_upload(self, path, key, bucket):
        """
        Tar the local file or directory and upload to s3

        :param path: local file or directory
        :type path: str
        :param key: s3 key
        :type key: str
        :param bucket: s3 bucket
        :type bucket: str
        :return: None
        """
        with tempfile.TemporaryFile() as temp_file:
            if os.path.isdir(path):
                files = [os.path.join(path, name) for name in os.listdir(path)]
            else:
                files = [path]
            with tarfile.open(mode='w:gz', fileobj=temp_file) as tar_file:
                for f in files:
                    tar_file.add(f, arcname=os.path.basename(f))
            temp_file.seek(0)
            self.s3_hook.load_file_obj(temp_file, key, bucket, replace=True)


    def configure_s3_resources(self, config):
        """
        Extract the S3 operations from the configuration and execute them.

        :param config: config of SageMaker operation
        :type config: dict
        :rtype: dict
        """
        s3_operations = config.pop('S3Operations', None)

        if s3_operations is not None:
            create_bucket_ops = s3_operations.get('S3CreateBucket', [])
            upload_ops = s3_operations.get('S3Upload', [])
            for op in create_bucket_ops:
                self.s3_hook.create_bucket(bucket_name=op['Bucket'])
            for op in upload_ops:
                if op['Tar']:
                    self.tar_and_s3_upload(op['Path'], op['Key'],
                                           op['Bucket'])
                else:
                    self.s3_hook.load_file(op['Path'], op['Key'],
                                           op['Bucket'])


    def check_s3_url(self, s3url):
        """
        Check if an S3 URL exists

        :param s3url: S3 url
        :type s3url: str
        :rtype: bool
        """
        bucket, key = S3Hook.parse_s3_url(s3url)
        if not self.s3_hook.check_for_bucket(bucket_name=bucket):
            raise AirflowException(
                "The input S3 Bucket {} does not exist ".format(bucket))
        if key and not self.s3_hook.check_for_key(key=key, bucket_name=bucket)\
           and not self.s3_hook.check_for_prefix(
                prefix=key, bucket_name=bucket, delimiter='/'):
            # check if s3 key exists in the case user provides a single file
            # or if s3 prefix exists in the case user provides multiple files in
            # a prefix
            raise AirflowException("The input S3 Key "
                                   "or Prefix {} does not exist in the Bucket {}"
                                   .format(s3url, bucket))
        return True


    def check_training_config(self, training_config):
        """
        Check if a training configuration is valid

        :param training_config: training_config
        :type training_config: dict
        :return: None
        """
        for channel in training_config['InputDataConfig']:
            self.check_s3_url(channel['DataSource']['S3DataSource']['S3Uri'])


    def check_tuning_config(self, tuning_config):
        """
        Check if a tuning configuration is valid

        :param tuning_config: tuning_config
        :type tuning_config: dict
        :return: None
        """
        for channel in tuning_config['TrainingJobDefinition']['InputDataConfig']:
            self.check_s3_url(channel['DataSource']['S3DataSource']['S3Uri'])


    def get_conn(self):
        """
        Establish an AWS connection for SageMaker

        :rtype: :py:class:`SageMaker.Client`
        """
        return self.get_client_type('sagemaker')


    def get_log_conn(self):
        """
        Establish an AWS connection for retrieving logs during training

        :rtype: :py:class:`CloudWatchLog.Client`
        """
        config = botocore.config.Config(retries={'max_attempts': 15})
        return self.get_client_type('logs', config=config)


    def log_stream(self, log_group, stream_name, start_time=0, skip=0):
        """
        A generator for log items in a single stream. This will yield all the
        items that are available at the current moment.

        :param log_group: The name of the log group.
        :type log_group: str
        :param stream_name: The name of the specific stream.
        :type stream_name: str
        :param start_time: The time stamp value to start reading the logs from (default: 0).
        :type start_time: int
        :param skip: The number of log entries to skip at the start (default: 0).
            This is for when there are multiple entries at the same timestamp.
        :type skip: int
        :rtype: dict
        :return: | A CloudWatch log event with the following key-value pairs:
                 |   'timestamp' (int): The time in milliseconds of the event.
                 |   'message' (str): The log event data.
                 |   'ingestionTime' (int): The time in milliseconds the event was ingested.
        """

        next_token = None

        event_count = 1
        while event_count > 0:
            if next_token is not None:
                token_arg = {'nextToken': next_token}
            else:
                token_arg = {}

            response = self.get_log_conn().get_log_events(logGroupName=log_group,
                                                          logStreamName=stream_name,
                                                          startTime=start_time,
                                                          startFromHead=True,
                                                          **token_arg)
            next_token = response['nextForwardToken']
            events = response['events']
            event_count = len(events)
            if event_count > skip:
                events = events[skip:]
                skip = 0
            else:
                skip = skip - event_count
                events = []
            for ev in events:
                yield ev


    def multi_stream_iter(self, log_group, streams, positions=None):
        """
        Iterate over the available events coming from a set of log streams in a single log group
        interleaving the events from each stream so they're yielded in timestamp order.

        :param log_group: The name of the log group.
        :type log_group: str
        :param streams: A list of the log stream names. The position of the stream in this list is
            the stream number.
        :type streams: list
        :param positions: A list of pairs of (timestamp, skip) which represents the last record
            read from each stream.
        :type positions: list
        :return: A tuple of (stream number, cloudwatch log event).
        """
        positions = positions or {s: Position(timestamp=0, skip=0) for s in streams}
        event_iters = [self.log_stream(log_group, s, positions[s].timestamp, positions[s].skip)
                       for s in streams]
        events = [next(s) if s else None for s in event_iters]

        while any(events):
            i = argmin(events, lambda x: x['timestamp'] if x else 9999999999)
            yield (i, events[i])
            try:
                events[i] = next(event_iters[i])
            except StopIteration:
                events[i] = None


    def create_training_job(self, config, wait_for_completion=True, print_log=True,
                            check_interval=30, max_ingestion_time=None):
        """
        Create a training job

        :param config: the config for training
        :type config: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :type wait_for_completion: bool
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: A response to training job creation
        """

        self.check_training_config(config)

        response = self.get_conn().create_training_job(**config)
        if print_log:
            self.check_training_status_with_log(config['TrainingJobName'],
                                                self.non_terminal_states,
                                                self.failed_states,
                                                wait_for_completion,
                                                check_interval, max_ingestion_time
                                                )
        elif wait_for_completion:
            describe_response = self.check_status(config['TrainingJobName'],
                                                  'TrainingJobStatus',
                                                  self.describe_training_job,
                                                  check_interval, max_ingestion_time
                                                  )

            billable_time = \
                (describe_response['TrainingEndTime'] - describe_response['TrainingStartTime']) * \
                describe_response['ResourceConfig']['InstanceCount']
            self.log.info('Billable seconds:{}'.format(int(billable_time.total_seconds()) + 1))

        return response


    def create_tuning_job(self, config, wait_for_completion=True,
                          check_interval=30, max_ingestion_time=None):
        """
        Create a tuning job

        :param config: the config for tuning
        :type config: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :param wait_for_completion: bool
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: A response to tuning job creation
        """

        self.check_tuning_config(config)

        response = self.get_conn().create_hyper_parameter_tuning_job(**config)
        if wait_for_completion:
            self.check_status(config['HyperParameterTuningJobName'],
                              'HyperParameterTuningJobStatus',
                              self.describe_tuning_job,
                              check_interval, max_ingestion_time
                              )
        return response


    def create_transform_job(self, config, wait_for_completion=True,
                             check_interval=30, max_ingestion_time=None):
        """
        Create a transform job

        :param config: the config for transform job
        :type config: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :type wait_for_completion: bool
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: A response to transform job creation
        """

        self.check_s3_url(config['TransformInput']['DataSource']['S3DataSource']['S3Uri'])

        response = self.get_conn().create_transform_job(**config)
        if wait_for_completion:
            self.check_status(config['TransformJobName'],
                              'TransformJobStatus',
                              self.describe_transform_job,
                              check_interval, max_ingestion_time
                              )
        return response


    def create_model(self, config):
        """
        Create a model job

        :param config: the config for model
        :type config: dict
        :return: A response to model creation
        """

        return self.get_conn().create_model(**config)

    def create_endpoint_config(self, config):
        """
        Create an endpoint config

        :param config: the config for endpoint-config
        :type config: dict
        :return: A response to endpoint config creation
        """

        return self.get_conn().create_endpoint_config(**config)


    def create_endpoint(self, config, wait_for_completion=True,
                        check_interval=30, max_ingestion_time=None):
        """
        Create an endpoint

        :param config: the config for endpoint
        :type config: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :type wait_for_completion: bool
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: A response to endpoint creation
        """

        response = self.get_conn().create_endpoint(**config)
        if wait_for_completion:
            self.check_status(config['EndpointName'],
                              'EndpointStatus',
                              self.describe_endpoint,
                              check_interval, max_ingestion_time,
                              non_terminal_states=self.endpoint_non_terminal_states
                              )
        return response


    def update_endpoint(self, config, wait_for_completion=True,
                        check_interval=30, max_ingestion_time=None):
        """
        Update an endpoint

        :param config: the config for endpoint
        :type config: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :type wait_for_completion: bool
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: A response to endpoint update
        """

        response = self.get_conn().update_endpoint(**config)
        if wait_for_completion:
            self.check_status(config['EndpointName'],
                              'EndpointStatus',
                              self.describe_endpoint,
                              check_interval, max_ingestion_time,
                              non_terminal_states=self.endpoint_non_terminal_states
                              )
        return response

    def describe_training_job(self, name):
        """
        Return the training job info associated with the name

        :param name: the name of the training job
        :type name: str
        :return: A dict contains all the training job info
        """

        return self.get_conn().describe_training_job(TrainingJobName=name)


    def describe_training_job_with_log(self, job_name, positions, stream_names,
                                       instance_count, state, last_description,
                                       last_describe_job_call):
        """
        Return the training job info associated with job_name and print CloudWatch logs
        """
        log_group = '/aws/sagemaker/TrainingJobs'

        if len(stream_names) < instance_count:
            # Log streams are created whenever a container starts writing to stdout/err, so this list
            # may be dynamic until we have a stream for every instance.
            logs_conn = self.get_log_conn()
            try:
                streams = logs_conn.describe_log_streams(
                    logGroupName=log_group,
                    logStreamNamePrefix=job_name + '/',
                    orderBy='LogStreamName',
                    limit=instance_count
                )
                stream_names = [s['logStreamName'] for s in streams['logStreams']]
                positions.update([(s, Position(timestamp=0, skip=0))
                                  for s in stream_names if s not in positions])
            except logs_conn.exceptions.ResourceNotFoundException:
                # On the very first training job run on an account, there's no log group until
                # the container starts logging, so ignore any errors thrown about that
                pass

        if len(stream_names) > 0:
            for idx, event in self.multi_stream_iter(log_group, stream_names, positions):
                self.log.info(event['message'])
                ts, count = positions[stream_names[idx]]
                if event['timestamp'] == ts:
                    positions[stream_names[idx]] = Position(timestamp=ts, skip=count + 1)
                else:
                    positions[stream_names[idx]] = Position(timestamp=event['timestamp'], skip=1)

        if state == LogState.COMPLETE:
            return state, last_description, last_describe_job_call

        if state == LogState.JOB_COMPLETE:
            state = LogState.COMPLETE
        elif time.time() - last_describe_job_call >= 30:
            description = self.describe_training_job(job_name)
            last_describe_job_call = time.time()

            if secondary_training_status_changed(description, last_description):
                self.log.info(secondary_training_status_message(description, last_description))
                last_description = description

            status = description['TrainingJobStatus']

            if status not in self.non_terminal_states:
                state = LogState.JOB_COMPLETE
        return state, last_description, last_describe_job_call


    def describe_tuning_job(self, name):
        """
        Return the tuning job info associated with the name

        :param name: the name of the tuning job
        :type name: string
        :return: A dict contains all the tuning job info
        """

        return self.get_conn().describe_hyper_parameter_tuning_job(HyperParameterTuningJobName=name)


    def describe_model(self, name):
        """
        Return the SageMaker model info associated with the name

        :param name: the name of the SageMaker model
        :type name: string
        :return: A dict contains all the model info
        """

        return self.get_conn().describe_model(ModelName=name)


    def describe_transform_job(self, name):
        """
        Return the transform job info associated with the name

        :param name: the name of the transform job
        :type name: string
        :return: A dict contains all the transform job info
        """

        return self.get_conn().describe_transform_job(TransformJobName=name)


    def describe_endpoint_config(self, name):
        """
        Return the endpoint config info associated with the name

        :param name: the name of the endpoint config
        :type name: string
        :return: A dict contains all the endpoint config info
        """

        return self.get_conn().describe_endpoint_config(EndpointConfigName=name)


    def describe_endpoint(self, name):
        """
        :param name: the name of the endpoint
        :type name: string
        :return: A dict contains all the endpoint info
        """

        return self.get_conn().describe_endpoint(EndpointName=name)


    def check_status(self, job_name, key,
                     describe_function, check_interval,
                     max_ingestion_time,
                     non_terminal_states=None):
        """
        Check status of a SageMaker job

        :param job_name: name of the job to check status
        :type job_name: str
        :param key: the key of the response dict
            that points to the state
        :type key: str
        :param describe_function: the function used to retrieve the status
        :type describe_function: python callable
        :param args: the arguments for the function
        :param check_interval: the time interval in seconds which the operator
            will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :param non_terminal_states: the set of nonterminal states
        :type non_terminal_states: set
        :return: response of describe call after job is done
        """
        if not non_terminal_states:
            non_terminal_states = self.non_terminal_states

        sec = 0
        running = True

        while running:
            time.sleep(check_interval)
            sec = sec + check_interval

            try:
                response = describe_function(job_name)
                status = response[key]
                self.log.info('Job still running for %s seconds... '
                              'current status is %s' % (sec, status))
            except KeyError:
                raise AirflowException('Could not get status of the SageMaker job')
            except ClientError:
                raise AirflowException('AWS request failed, check logs for more info')

            if status in non_terminal_states:
                running = True
            elif status in self.failed_states:
                raise AirflowException('SageMaker job failed because %s' % response['FailureReason'])
            else:
                running = False

            if max_ingestion_time and sec > max_ingestion_time:
                # ensure that the job gets killed if the max ingestion time is exceeded
                raise AirflowException('SageMaker job took more than %s seconds', max_ingestion_time)

        self.log.info('SageMaker Job Compeleted')
        response = describe_function(job_name)
        return response


    def check_training_status_with_log(self, job_name, non_terminal_states, failed_states,
                                       wait_for_completion, check_interval, max_ingestion_time):
        """
        Display the logs for a given training job, optionally tailing them until the
        job is complete.

        :param job_name: name of the training job to check status and display logs for
        :type job_name: str
        :param non_terminal_states: the set of non_terminal states
        :type non_terminal_states: set
        :param failed_states: the set of failed states
        :type failed_states: set
        :param wait_for_completion: Whether to keep looking for new log entries
            until the job completes
        :type wait_for_completion: bool
        :param check_interval: The interval in seconds between polling for new log entries and job completion
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
            SageMaker jobs that run longer than this will fail. Setting this to
            None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: None
        """

        sec = 0
        description = self.describe_training_job(job_name)
        self.log.info(secondary_training_status_message(description, None))
        instance_count = description['ResourceConfig']['InstanceCount']
        status = description['TrainingJobStatus']

        stream_names = []  # The list of log streams
        positions = {}     # The current position in each stream, map of stream name -> position

        job_already_completed = status not in non_terminal_states

        state = LogState.TAILING if wait_for_completion and not job_already_completed else LogState.COMPLETE

        # The loop below implements a state machine that alternates between checking the job status and
        # reading whatever is available in the logs at this point. Note, that if we were called with
        # wait_for_completion == False, we never check the job status.
        #
        # If wait_for_completion == TRUE and job is not completed, the initial state is TAILING
        # If wait_for_completion == FALSE, the initial state is COMPLETE
        # (doesn't matter if the job really is complete).
        #
        # The state table:
        #
        # STATE               ACTIONS                        CONDITION             NEW STATE
        # ----------------    ----------------               -----------------     ----------------
        # TAILING             Read logs, Pause, Get status   Job complete          JOB_COMPLETE
        #                                                    Else                  TAILING
        # JOB_COMPLETE        Read logs, Pause               Any                   COMPLETE
        # COMPLETE            Read logs, Exit                                      N/A
        #
        # Notes:
        # - The JOB_COMPLETE state forces us to do an extra pause and read any items that
        # got to Cloudwatch after the job was marked complete.
        last_describe_job_call = time.time()
        last_description = description

        while True:
            time.sleep(check_interval)
            sec = sec + check_interval

            state, last_description, last_describe_job_call = \
                self.describe_training_job_with_log(job_name, positions, stream_names,
                                                    instance_count, state, last_description,
                                                    last_describe_job_call)
            if state == LogState.COMPLETE:
                break

            if max_ingestion_time and sec > max_ingestion_time:
                # ensure that the job gets killed if the max ingestion time is exceeded
                raise AirflowException('SageMaker job took more than %s seconds', max_ingestion_time)

        if wait_for_completion:
            status = last_description['TrainingJobStatus']
            if status in failed_states:
                reason = last_description.get('FailureReason', '(No reason provided)')
                raise AirflowException('Error training {}: {} Reason: {}'.format(job_name, status, reason))
            billable_time = (last_description['TrainingEndTime'] - last_description['TrainingStartTime']) \
                * instance_count
            self.log.info('Billable seconds:{}'.format(int(billable_time.total_seconds()) + 1))

# Creating schema if inexistent
def create_schema(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    log.info('Initialised connection')
    sql_queries = """

    CREATE SCHEMA IF NOT EXISTS epc_schema;

    DROP TABLE IF EXISTS epc_schema.london_streets;
    CREATE TABLE IF NOT EXISTS epc_schema.london_streets(
        "street_id" numeric,
        "street_name" varchar(256),
        "url" varchar(256)
    );

    DROP TABLE IF EXISTS epc_schema.epc_rating;
    CREATE TABLE IF NOT EXISTS epc_schema.epc_rating(
        "street_name" varchar(256),
        "url" varchar(256),
        "average_epc" varchar(256)
    );

    CREATE SCHEMA IF NOT EXISTS schema_housing;
    DROP TABLE IF EXISTS schema_housing.dexters;
    CREATE TABLE IF NOT EXISTS schema_housing.dexters(
        "ad_id" numeric,
        "street_name" varchar(256),
        "price" numeric,
        "address" varchar(256),
        "bedrooms" numeric,
        "bathrooms" numeric,
        "reception" numeric,
        "link" varchar(256),
        "subway_station" varchar(256),
        "distance" numeric,
        "tube_line" varchar(256)

    );
    """

    cursor.execute(sql_queries)
    conn.commit()
    log.info("Created Schemas and Tables for EPC and for Dexters")

# Webscraping street_names in all of London and adding the

#attempting to scrape the streets for the entire alphabet
def web_scraping_all_london_streets(**kwargs):

    import pandas as pd
    import itertools
    import string
    import numpy as np
    from bs4 import BeautifulSoup #requires pip install
    import requests
    import re
    from re import sub
    import io
    from statistics import mode

    list_london_street_names=[]
    for letter in list(string.ascii_lowercase):

        london_streetmap='http://london.streetmapof.co.uk/'
        streetmap_url=london_streetmap+letter+'/'


        map_html_text = requests.get(streetmap_url).text
        soup_map = BeautifulSoup(map_html_text, 'lxml')

        #find street names in the map:
        map_ads=soup_map.find_all('td', { "valign": "top" })


        # ad = map_ads[1]
        count_pages=[]
        [count_pages.append(a) for a in soup_map.find_all('a', href=True) if a['href'].startswith(f'/{letter}/')]


        log.info("have determined how many pages there are for each letter")

        list_starting_with_letter=[]
        if len(count_pages)>0:
            list_starting_with_letter=[]
            for i in range(len(count_pages)):
                if i==0:
                    streetmap_url=london_streetmap+letter+'/'

                    map_html_text = requests.get(streetmap_url).text
                    soup_map = BeautifulSoup(map_html_text, 'lxml')
                    map_ads=soup_map.find_all('td', { "valign": "top" })
                    for k in range(len(map_ads)):
                        ad = map_ads[k]


                        for a in ad.find_all('a', href=True):
                            if a['href'].startswith(f'/{letter}/'):
                                count_pages.append(a)
                            else:
                                list_starting_with_letter.append(a.get_text(strip=True))
                else:
                    streetmap_url=london_streetmap+letter+'/'+str(i+1)

                    map_html_text = requests.get(streetmap_url).text
                    soup_map = BeautifulSoup(map_html_text, 'lxml')
                    map_ads=soup_map.find_all('td', { "valign": "top" })

                    for k in range(len(map_ads)):
                        ad = map_ads[k]


                        for a in ad.find_all('a', href=True):
                            if a['href'].startswith(f'/{letter}/'):
                                count_pages.append(a)
                            else:
                                list_starting_with_letter.append(a.get_text(strip=True))
            len(list(set(list_starting_with_letter)))
            list_london_street_names.append(list(set(list_starting_with_letter)))
        else:
            list_starting_with_letter=[]
            streetmap_url=london_streetmap+letter+'/'

            map_html_text = requests.get(streetmap_url).text
            soup_map = BeautifulSoup(map_html_text, 'lxml')
            map_ads=soup_map.find_all('td', { "valign": "top" })
            for k in range(len(map_ads)):
                ad = map_ads[k]


                for a in ad.find_all('a', href=True):
                    if a['href'].startswith(f'/{letter}/'):
                        break
                    else:
                        list_starting_with_letter.append(a.get_text(strip=True))
            len(list(set(list_starting_with_letter)))
            list_london_street_names.append(list(set(list_starting_with_letter)))

    log.info("finalised list of London street names")
    #flatten the list of lists for london street names
    flatten = itertools.chain.from_iterable
    list_london_street_names=list(flatten(list_london_street_names))

    #convert to a dataframe
    df=pd.DataFrame(list_london_street_names, columns=['street_name'])

    #adding another column to the dataframe with all streets converted to links suitable for web scraping EPC ratings:
    list_epc_urls=[]
    epc_url='https://find-energy-certificate.digital.communities.gov.uk/find-a-certificate/search-by-street-name-and-town?street_name='
    town_url='&town=London'
    epc_avg=[]

    for street_name in df['street_name'].tolist():
        url_street_name=street_name.replace(' ','+')
        list_epc_urls.append(epc_url+url_street_name+town_url)

    df['url']=np.array(list_epc_urls)

    df.reset_index(level=0, inplace=True)
    df=df.rename(columns={'index': 'street_id'})

    log.info("finalised list of London street names URLs")

    #Saving CSV to S3

    #Establishing S3 connection

    s3 = S3Hook(kwargs['aws_conn_id'])

    bucket_name = kwargs['bucket_name']

    #name of the file

    key = Variable.get("housing_epc_rating", deserialize_json=True)['key4']+".csv" #using this format as we would like to attempt to use datetime to identify files



    # Prepare the file to send to s3

    csv_buffer_london_streets = io.StringIO()

    #Ensuring the CSV files treats "NAN" as null values

    london_streets_csv=df.to_csv(csv_buffer_london_streets, index=False, header=True)



    # Save the pandas dataframe as a csv to s3

    s3 = s3.get_resource_type('s3')



    # Get the data type object from pandas dataframe, key and connection object to s3 bucket

    data = csv_buffer_london_streets.getvalue()



    print("Saving CSV file")

    object = s3.Object(bucket_name, key)



    # Write the file to S3 bucket in specific path defined in key

    object.put(Body=data)



    log.info('Finished saving the scraped london streets data to s3')



    return


# Saving file with London Streets and URLs to postgreSQL database
def save_london_streets_to_postgres_db(**kwargs):

    import pandas as pd
    import io


    #Establishing connection to S3 bucket
    bucket_name = kwargs['bucket_name']
    key = Variable.get("housing_epc_rating", deserialize_json=True)['key4']+".csv"
    s3 = S3Hook(kwargs['aws_conn_id'])
    log.info("Established connection to S3 bucket")


    # Get the task instance
    task_instance = kwargs['ti']
    print(task_instance)


    # Read the content of the key from the bucket
    csv_bytes_london_streets = s3.read_key(key, bucket_name)
    # Read the CSV
    df_london_streets = pd.read_csv(io.StringIO(csv_bytes_london_streets ))#, encoding='utf-8')

    log.info('passing London streets and URLs data from S3 bucket')

    # Connect to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id=kwargs["postgres_conn_id"], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    log.info('Initialised connection to Postgres DB')

    #Required code for clearing an error related to int64
    import numpy
    from psycopg2.extensions import register_adapter, AsIs
    def addapt_numpy_float64(numpy_float64):
        return AsIs(numpy_float64)
    def addapt_numpy_int64(numpy_int64):
        return AsIs(numpy_int64)
    register_adapter(numpy.float64, addapt_numpy_float64)
    register_adapter(numpy.int64, addapt_numpy_int64)

    log.info('Loading row by row into database')
    # #Removing NaN values and converting to NULL:

    df_london_streets = df_london_streets.where(pd.notnull(df_london_streets), None)

    s = """INSERT INTO epc_schema.london_streets(street_id, street_name, url) VALUES (%s, %s, %s)"""
    for index in range(len(df_london_streets)):
        obj = []

        obj.append([df_london_streets.street_id[index],
                   df_london_streets.street_name[index],
                   df_london_streets.url[index]])

        cursor.executemany(s, obj)
        conn.commit()

    log.info('Finished saving the scraped London streets data to postgres database')
    cursor.close()
    conn.close()

def get_sql_merged_table(**kwargs):

    import pandas as pd
    import io

    import itertools
    import string
    import numpy as np
    from bs4 import BeautifulSoup #requires pip install
    import requests
    import re
    from re import sub
    import io
    from statistics import mode

    request="SELECT schema_housing.dexters.street_name, url from schema_housing.dexters JOIN epc_schema.london_streets on schema_housing.dexters.street_name=epc_schema.london_streets.street_name;"
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(request)
##    conn.commit()
##    merged_table=pd.DataFrame(list(cursor.fetchall()), columns =[col[0] for col in cursor.description]) #this works but shows an empty csv
    merged_table = pd.read_sql_query(request, conn)
##    merged_table=cursor.fetchall()
    log.info('Finished SQL merge on database')
##    merged_table

    #Scraping average EPC ratings

    #Iterating through the urls in the dataframe:
    epc_avg=[]
    for url in merged_table['url'].tolist():

        html_text = requests.get(url).text
        soup = BeautifulSoup(html_text, 'lxml')
        epc_ads=soup.find_all('tr', class_ = 'govuk-table__row')

        epc_id_ad = 0
        flat_address_final=[]
        epc_rating=[]
        #iterating through each ad to find the street names and ensure that there is a street name listed in the EPC database
        for k in range(len(epc_ads)):
            ad = epc_ads[k]
            flat_address = ad.find_all('a', class_='govuk-link')

            if len(flat_address) !=0:
                flat_address_final.append(flat_address[0].get_text(strip=True))
            else:
                continue

            epc_rating.append(ad.find_all('td', class_='govuk-table__cell')[0].get_text(strip=True))

            epc_id_ad += 1
        #For some streets, there are no results in the gov website for average energy ratings
        if len(epc_rating)!=0:
            epc_avg.append(mode(epc_rating))
        else:
            epc_avg.append(0)
    df_average_epc=pd.DataFrame(epc_avg, columns=['average_epc'])
    street_name_epc=pd.concat([merged_table, df_average_epc],axis=1)
    street_name_epc

    log.info('Finished scraping EPC ratings from merged SQL table')


    #Saving merged table and EPC ratings as CSV to S3

    #Establishing S3 connection

    s3 = S3Hook(kwargs['aws_conn_id'])
    bucket_name = kwargs['bucket_name']

    #name of the file
#    key = Variable.get("housing_epc_rating", deserialize_json=True)['key4']+".csv" #using this format as we would like to attempt to use datetime to identify files
    key = "output/street_name_epc_ratings"+".csv" #test name

    # Prepare the file to send to s3
    merged_table_csv = io.StringIO()

    new_merged_table_csv=street_name_epc.to_csv(merged_table_csv, index=False, header=True)

    # Save the pandas dataframe as a csv to s3

    s3 = s3.get_resource_type('s3')

    # Get the data type object from pandas dataframe, key and connection object to s3 bucket

    data = merged_table_csv.getvalue()

    print("Saving CSV file")
    object = s3.Object(bucket_name, key)

    # Write the file to S3 bucket in specific path defined in key

    object.put(Body=data)


    log.info('Finished saving new merged table with energy ratings data to s3')

    cursor.close()
    conn.close()


    # Connect to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id=kwargs["postgres_conn_id"], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    log.info('Initialised connection to Postgres DB')

    #Required code for clearing an error related to int64
    import numpy
    from psycopg2.extensions import register_adapter, AsIs
    def addapt_numpy_float64(numpy_float64):
        return AsIs(numpy_float64)
    def addapt_numpy_int64(numpy_int64):
        return AsIs(numpy_int64)
    register_adapter(numpy.float64, addapt_numpy_float64)
    register_adapter(numpy.int64, addapt_numpy_int64)

    log.info('Loading row by row into database')
    # #Removing NaN values and converting to NULL:

    street_name_epc = street_name_epc.where(pd.notnull(street_name_epc), None)

    s = """INSERT INTO epc_schema.epc_rating(street_name, url, average_epc) VALUES (%s, %s, %s)"""
    for index in range(len(street_name_epc)):
        obj = []

        obj.append([street_name_epc.street_name[index],
                   street_name_epc.url[index],
                   street_name_epc.average_epc[index]])

        cursor.executemany(s, obj)
        conn.commit()

    log.info('Finished saving the scraped London EPC ratings data to postgres database')


    sql_queries = """

    CREATE TABLE epc_schema.epc_rating_copy as (Select street_name as str_name, url, average_epc from epc_schema.epc_rating);

    CREATE TABLE epc_schema.dexters_epc AS (SELECT * from schema_housing.dexters JOIN epc_schema.epc_rating_copy on schema_housing.dexters.street_name=epc_schema.epc_rating_copy.str_name);

    DROP TABLE epc_schema.epc_rating_copy;

    ALTER TABLE epc_schema.dexters_epc
    DROP COLUMN str_name,
    DROP COLUMN url;
    """
    log.info('Finished creating new table in database that merges housing scraper with EPC rating and drops irrelevant columns')
    cursor.execute(sql_queries)

    cursor.close()
    conn.close()

    # Get the output of the bash task
    task_instance = kwargs['ti']
    scraped_dexters_data = task_instance.xcom_pull(task_ids="web_scraping_task_dexters")
    log.info('xcom from web_scraping_task_dexters:{0}'.format(scraped_dexters_data))

    clean_dexters = pd.DataFrame.from_dict(scraped_dexters_data)

    # Load the list of dictionaries with the scraped data from the previous task into a pandas dataframe
    log.info('Loading scraped data into pandas dataframe')

    #Combining the two new dataframes as per the Postgres database, and saving as CSV so we can use for Sagemaker
    dexters_epc=pd.merge(clean_dexters,street_name_epc, on='street_name')
    dexters_epc=dexters_epc.drop('url', axis=1)

    #Cleaning file and preparing for ML

    def clean_df_for_ML(dexters_epc_ratings):
        efficient_ratings=['A','B']
        dexters_epc_ratings['efficient'] = np.where(dexters_epc_ratings['average_epc'].isin(efficient_ratings), 1, 0)
        dexters_epc_ratings = pd.get_dummies(data=dexters_epc_ratings, columns=['average_epc', "tube_line"])
        return dexters_epc_ratings
    dexters_epc=clean_df_for_ML(dexters_epc)

    #Saving final table with Dexters scraper and EPC ratings as CSV to S3

    #Establishing S3 connection

    s3 = S3Hook(kwargs['aws_conn_id'])
    bucket_name = kwargs['bucket_name']

    #name of the file
#    key = Variable.get("housing_epc_rating", deserialize_json=True)['key4']+".csv" #using this format as we would like to attempt to use datetime to identify files
    key = "output/dexters_epc_ratings"+".csv" #test name

    # Prepare the file to send to s3
    dexters_epc_ratings = io.StringIO()

    dexters_epc_ratings_csv=dexters_epc.to_csv(dexters_epc_ratings, index=False, header=True)

    # Save the pandas dataframe as a csv to s3

    s3 = s3.get_resource_type('s3')

    # Get the data type object from pandas dataframe, key and connection object to s3 bucket

    data = dexters_epc_ratings.getvalue()

    print("Saving CSV file")
    object = s3.Object(bucket_name, key)

    # Write the file to S3 bucket in specific path defined in key

    object.put(Body=data)


    log.info('Finished saving Final dexters table with energy ratings data to s3')

    cursor.close()
    conn.close()



##
##    cursor.execute(sql_queries)
##    conn.commit()


# Webscraping dexters
def web_scraping_function_dexters(**kwargs):

        # Import packages
        import pandas as pd
        import numpy as np
        import datetime
        from bs4 import BeautifulSoup #requires pip install
        import requests #requires pip install
        import re
        import time

        import io
        # document time
        time_started = str(datetime.datetime.now()).replace(" ","_").replace(":","-")[0:19]
        ## Define list of subway stations
        Underground_lines = ['Bakerloo', 'Central', 'Circle', 'District', 'DLR', 'Hammersmith & City',
                         'Jubilee', 'Metropolitan', 'Northern', 'Piccadilly', 'Victoria', 'Waterloo & City']

        ## Function to extract characteristics on each ad from the main webpage
        def feature_extract(html_text):

            soup = BeautifulSoup(html_text, 'lxml')

            ## Parse for the different divisions within the add

            # ads = soup.find_all('div', class_ = 'result-content') #searches for 'div' and is filtered by the CSS-snippet
            ads = soup.find_all('li', class_ = 'result item for-sale infinite-item')#searches for 'div' and is filtered by the CSS-snippet
            ## Set-up for the loop
            results = {} #create nested dictionary to store the results
            id_ad = 0 #insert ad_ID to distinguish between each ad

            ## Loop across all ads
            for k in range(len(ads)):
                ad = ads[k]
                id_ad += 1
                results[id_ad] = {}

                ## Extracting features from the ad
                name = ad.find('h3').a.contents[0]
                try:
                    price = ad.find('span', class_ = 'price-qualifier').text #catches the price WITHIN one ad
                except:
                    continue
                address = ad.find('span', class_ = 'address-area-post').text

                # Number of bedrooms extracted from string
                try:
                    bedrooms = ad.find('li', class_ = 'Bedrooms').text
                except:
                    continue
                bedrooms_nbr = int(bedrooms.split()[0])

                # Number of bedrooms extracted from string
                bathrooms_str = str(ad.find('li',class_ = 'Bathrooms'))
                bathrooms_nbr = re.findall(r'\d+', bathrooms_str)
                bathrooms_nbr2 = int(bathrooms_nbr[0] if len(bathrooms_nbr)!= 0  else 0)

                # Number of bedrooms extracted from string
                reception_str = str(ad.find('li',class_ = 'Receptions'))
                reception_nbr = re.findall(r'\d+', reception_str)
                reception_nbr2 = int(reception_nbr[0] if len(reception_nbr)!= 0  else 1)

                link = ad.find('h3').a.get("href")

                ad_id = ads[k]['data-property-id']

                # Create dictionary of results per ad id
                results[id_ad]['ad_id'] = ad_id
                results[id_ad]["street_name"] = name
                results[id_ad]["price"] = price
                results[id_ad]["address"] = address
                results[id_ad]["bedrooms"] = bedrooms_nbr
                results[id_ad]["bathrooms"] = bathrooms_nbr2
                results[id_ad]["reception"] = reception_nbr2
                results[id_ad]["link"] = ("https://www.dexters.co.uk" + link)

                # Create dataframe from dictionary of results
                df_houses = pd.DataFrame.from_dict(results, orient='index')

            return df_houses

        ## Function to create list of pages base on url and number of iterations desired
        def page_list(string, iterations):
            pages_list = []
            for i in range(iterations):
                pages_list.append(string + str(i+1))

            return pages_list

        ## Function to get the maximum number of listing on Dexter's website
        def page_max(url):
            html_text = requests.get(url).text
            soup = BeautifulSoup(html_text, 'lxml')
            amount = soup.find('span', class_ = 'marker-count has-results').text
            amount_num = re.sub('\D', '', amount)
            return int(amount_num)

        ## Function to launch scrapper on a specific webpage with number of pages to scrap
        def pages_scrap(main_page, iter_page, pages):
            max_pages = (page_max(main_page)/18)
            list_of_pages = page_list(iter_page, pages) # Create list of pages to scrape
            df_list = [] #Create list of dataframes to be concatenated by the end of the loop

            # Loop through all pages to create the different dataframes
            for page in list_of_pages:
                html_page = requests.get(page)
                html_page.encoding = 'utf-8'
                page = html_page.text
                df_ads = feature_extract(page)
                df_list.append(df_ads)

            # Concatenate the different dataframes
            df_results = pd.concat(df_list)
            df_results = df_results.drop_duplicates()
            df_results = df_results.reset_index(drop=True)

            print('Remaining number of page: ', int(max_pages - pages) )

            return df_results
        # 1.2 Subway related functions

        ## Function to extract subway info list from a house webpage on dexter
        def get_info_subway(link):
            html_text = requests.get(link).text
            soup = BeautifulSoup(html_text, 'lxml')
            subway = soup.find('ul', class_ = 'list-information').text

            return subway

        ## Function to get list of values for subway distances with string
        def sub_values(string):
            split = string.split('\n')
            list_1 = list(filter(None, split))

            list_2 = []
            for i in list_1:
                x = i.split('-')
                list_2.append(x)

            list_3 = [item.strip() for sublist in list_2 for item in sublist]
            list_4 = list_3[0:3]

            return list_3

        ## Function to get the closest stop on the tube if any
        def closest_line(list_of_lines):
            j = 0
            nearby_data = []
            for i in range(len(list_of_lines)):
                if list_of_lines[i] == 'London Underground' or list_of_lines[i] in Underground_lines and (j != 1 and i!=0):
                    if (' ' in list_of_lines[i-2]) == False :
                        nearby_data.append(list_of_lines[i-3])
                        nearby_data.append(list_of_lines[i-2])
                        nearby_data.append(list_of_lines[i-1])
                        nearby_data.append(list_of_lines[i])
                        j = 1

                        nearby_data[0] = (' '.join(nearby_data[0:2]))
                        del nearby_data[1]

                    else:
                        nearby_data.append(list_of_lines[i-2])
                        nearby_data.append(list_of_lines[i-1])
                        nearby_data.append(list_of_lines[i])
                        j = 1

            return nearby_data

        ## Function to populate datafrmae with closest tube stop name, distance, and related tube line
        def subway_per_house(df):
            #Create new empty (NaN) columns in the existing dataframe
            df = df.reindex(columns = df.columns.tolist() + ['subway_station','distance','tube_line'])

            #Loop through all lines of dataframe
            for i in range(len(df)):
                x = df['link'].iloc[i] #Get link of house page to scrape
                subs = get_info_subway(x) #Extract tube line info
                subs_2 = sub_values(subs) #Get list of subway station and distance
                subs_3 = closest_line(subs_2) #Extract closest tube station only

                # Populate dataframe if a tubeway station has been found or not
                if len(subs_3)!= 0:
                    df['subway_station'].iloc[i] = subs_3[0]
                    df['distance'].iloc[i] = subs_3[1]
                    df['tube_line'].iloc[i] = subs_3[2]
                else:
                    df['subway_station'].iloc[i] = np.NaN
                    df['distance'].iloc[i] = np.NaN
                    df['tube_line'].iloc[i] = np.NaN

            df = df.astype(str)

            return df

        #Functions to clean subway output
        def get_tube_dist(string):
            string_m = string.split(' ')
            num_val = string_m[-1]

            return num_val
        def strip_tube(string):
            string_m = string.split(' ')
            string_m = string_m[:-1]
            string_m = ' '.join(string_m)

            return string_m
        def hasNumbers(inputString):
            return any(char.isdigit() for char in inputString)

        ## Function to clean subway stops when too many words in the string
        def clean_tube_stop_string(string):
            forbiddden_words = ['London Overground', 'Railway', 'Network Rail', 'Tramlink']
            count_forbidden = 0

            for j in forbiddden_words:
                if count_forbidden == 0:
                    if j in string:
                        string_update = string.split()[-1]
                        count_forbidden = 1
                    else:
                        string_update = string

            return(string_update)

        ## Function to input tube distance into the right column when value is in 'tube_stop'
        def clean_tube_dist(df):
            df['distance'] = df['distance'].astype('str')

            errors  = df[df.loc[:, 'distance'].map(hasNumbers) == False].copy()
            errors_2 = errors.loc[errors['subway_station'] != 'NaN'].copy()
            errors_2.loc[:, 'distance'] = errors_2.loc[:, 'subway_station'].map(get_tube_dist)
            errors_2.loc[:, 'subway_station'] = errors_2.loc[:, 'subway_station'].map(strip_tube)
            errors_2

            #Create copy of original df for modification
            df_copy = df.copy()

            # replace values in final df
            for i in errors_2.index:
                df_copy.loc[i] = errors_2.loc[i]

            return df_copy

        ## Functions to deal with Victoria tube stops (Victoria being both a tube stop and a tube line)
        def victoria_clean_stop(string):
            str_vic = 'Victoria'
            str_check = string.split()
            if str_check[0] == 'Victoria':
                str_return = str_check[1]
            else:
                str_return = str_vic

            return str_return
        def clean_tube_victoria(df):
            df['subway_station'] = df['subway_station'].astype('str')

            errors  = df[df['subway_station'].str.contains('Victoria')].copy()

            errors.loc[:, 'subway_station'] = errors.loc[:, 'subway_station'].map(victoria_clean_stop)

            #Create copy of original df for modification
            df_copy = df.copy()

            # Replace values in final df
            for i in errors.index:
                df_copy.loc[i] = errors.loc[i]

            return df_copy

        ## Final cleaning function to apply previous cleaning on 'tube_stop' and 'tube_dist' for the whole dataframe
        def clean_tube_stop(df):
            df_2 = df.copy()
            df_2 = clean_tube_dist(df_2)
            df_2['subway_station'] = df_2['subway_station'].astype('str')
            df_2['subway_station'] = df_2['subway_station'].map(clean_tube_stop_string)

            df_2 = clean_tube_victoria(df_2)
            # #Keep the ID of the add as index or not


            return df_2

        dexters_list_1 = pages_scrap('https://www.dexters.co.uk/property-sales/properties-for-sale-in-london',
                                    'https://www.dexters.co.uk/property-sales/properties-for-sale-in-london/page-', 1)


        ## Fetch subway related information from the previous dataframe
        output_list = subway_per_house(dexters_list_1)

        output_list

        cleaned = clean_tube_stop(output_list)
        cleaned

        #Cleaning the price and distance variables and converting to float
        cleaned['price'] = cleaned['price'].str.replace('£', '')
        cleaned['price'] = cleaned['price'].str.replace(',', '').astype(float)
        cleaned['distance'] = cleaned['distance'].str.replace('m', '').astype(float)


        cleaned['subway_station'].nunique()
        cleaned_dict = cleaned.to_dict(orient='records')

        log.info('Finished scraping the data')
        #create connection for uploading the file to S3

        #Establishing S3 connection
        s3 = S3Hook(kwargs['aws_conn_id'])
        bucket_name = kwargs['bucket_name']
        #name of the file
        key = Variable.get("housing_epc_rating", deserialize_json=True)['key2']+".csv"

        # Prepare the file to send to s3
        csv_buffer = io.StringIO()
        #Ensuring the CSV files treats "NAN" as null values
        cleaned_csv=cleaned.to_csv(csv_buffer, index=False)

        # Save the pandas dataframe as a csv to s3
        s3 = s3.get_resource_type('s3')

        # Get the data type object from pandas dataframe, key and connection object to s3 bucket
        data = csv_buffer.getvalue()

        print("Saving CSV file")
        object = s3.Object(bucket_name, key)

        # Write the file to S3 bucket in specific path defined in key
        object.put(Body=data)

        log.info('Finished saving the scraped data to s3')


        return cleaned_dict


# Saving Dexter file to postgreSQL database
def save_result_to_postgres_db_dexters(**kwargs):

    import pandas as pd
    import io

    #Establishing connection to S3 bucket
    bucket_name = kwargs['bucket_name']
    key = Variable.get("housing_epc_rating", deserialize_json=True)['key2']+".csv"
    s3 = S3Hook(kwargs['aws_conn_id'])
    log.info("Established connection to S3 bucket")


    # Get the task instance
    task_instance = kwargs['ti']
    print(task_instance)


    # Read the content of the key from the bucket
    csv_bytes = s3.read_key(key, bucket_name)
    # Read the CSV
    clean_dexters = pd.read_csv(io.StringIO(csv_bytes ))#, encoding='utf-8')

    log.info('passing dexters data from S3 bucket')

    # Connect to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id=kwargs["postgres_conn_id"], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    log.info('Initialised connection')

    #Required code for clearing an error related to int64
    import numpy
    from psycopg2.extensions import register_adapter, AsIs
    def addapt_numpy_float64(numpy_float64):
        return AsIs(numpy_float64)
    def addapt_numpy_int64(numpy_int64):
        return AsIs(numpy_int64)
    register_adapter(numpy.float64, addapt_numpy_float64)
    register_adapter(numpy.int64, addapt_numpy_int64)

    log.info('Loading row by row into database')
    # #Removing NaN values and converting to NULL:

    clean_dexters = clean_dexters.where(pd.notnull(clean_dexters), None)

    #Load the rows into the PostgresSQL database
    s = """INSERT INTO schema_housing.dexters( ad_id, street_name, price, address, bedrooms, bathrooms, reception, link, subway_station, distance, tube_line) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    for index in range(len(clean_dexters)):
        obj = []

        obj.append([clean_dexters.ad_id[index],
                   clean_dexters.street_name[index],
                   clean_dexters.price[index],
                   clean_dexters.address[index],
                   clean_dexters.bedrooms[index],
                   clean_dexters.bathrooms[index],
                   clean_dexters.reception[index],
                   clean_dexters.link[index],
                   clean_dexters.subway_station[index],
                   clean_dexters.distance[index],
                   clean_dexters.tube_line[index]])

        cursor.executemany(s, obj)
        conn.commit()

    log.info('Finished saving the dexters data to postgres database')

##
###Request Syntax for training job
##response = client.create_training_job(
##    TrainingJobName='training_job',
##    HyperParameters={
##        'string': 'string'
##    },
##    AlgorithmSpecification={
##        'TrainingImage': 'string',
##        'AlgorithmName': 'string',
##        'TrainingInputMode': 'Pipe'|'File',
##        'MetricDefinitions': [
##            {
##                'Name': 'string',
##                'Regex': 'string'
##            },
##        ],
##        'EnableSageMakerMetricsTimeSeries': True|False
##    },
##    RoleArn='string',
##    InputDataConfig=[
##        {
##            'ChannelName': 'string',
##            'DataSource': {
##                'S3DataSource': {
##                    'S3DataType': 'ManifestFile'|'S3Prefix'|'AugmentedManifestFile',
##                    'S3Uri': 'string',
##                    'S3DataDistributionType': 'FullyReplicated'|'ShardedByS3Key',
##                    'AttributeNames': [
##                        'string',
##                    ]
##                },
##                'FileSystemDataSource': {
##                    'FileSystemId': 'string',
##                    'FileSystemAccessMode': 'rw'|'ro',
##                    'FileSystemType': 'EFS'|'FSxLustre',
##                    'DirectoryPath': 'string'
##                }
##            },
##            'ContentType': 'string',
##            'CompressionType': 'None'|'Gzip',
##            'RecordWrapperType': 'None'|'RecordIO',
##            'InputMode': 'Pipe'|'File',
##            'ShuffleConfig': {
##                'Seed': 123
##            }
##        },
##    ],
##    OutputDataConfig={
##        'KmsKeyId': 'string',
##        'S3OutputPath': 'string'
##    },
##    ResourceConfig={
##        'InstanceType': 'ml.m4.xlarge'|'ml.m4.2xlarge'|'ml.m4.4xlarge'|'ml.m4.10xlarge'|'ml.m4.16xlarge'|'ml.g4dn.xlarge'|'ml.g4dn.2xlarge'|'ml.g4dn.4xlarge'|'ml.g4dn.8xlarge'|'ml.g4dn.12xlarge'|'ml.g4dn.16xlarge'|'ml.m5.large'|'ml.m5.xlarge'|'ml.m5.2xlarge'|'ml.m5.4xlarge'|'ml.m5.12xlarge'|'ml.m5.24xlarge'|'ml.c4.xlarge'|'ml.c4.2xlarge'|'ml.c4.4xlarge'|'ml.c4.8xlarge'|'ml.p2.xlarge'|'ml.p2.8xlarge'|'ml.p2.16xlarge'|'ml.p3.2xlarge'|'ml.p3.8xlarge'|'ml.p3.16xlarge'|'ml.p3dn.24xlarge'|'ml.p4d.24xlarge'|'ml.c5.xlarge'|'ml.c5.2xlarge'|'ml.c5.4xlarge'|'ml.c5.9xlarge'|'ml.c5.18xlarge'|'ml.c5n.xlarge'|'ml.c5n.2xlarge'|'ml.c5n.4xlarge'|'ml.c5n.9xlarge'|'ml.c5n.18xlarge',
##        'InstanceCount': 123,
##        'VolumeSizeInGB': 123,
##        'VolumeKmsKeyId': 'string'
##    },
##    VpcConfig={
##        'SecurityGroupIds': [
##            'string',
##        ],
##        'Subnets': [
##            'string',
##        ]
##    },
##    StoppingCondition={
##        'MaxRuntimeInSeconds': 123,
##        'MaxWaitTimeInSeconds': 123
##    },
##    Tags=[
##        {
##            'Key': 'string',
##            'Value': 'string'
##        },
##    ],
##    EnableNetworkIsolation=True|False,
##    EnableInterContainerTrafficEncryption=True|False,
##    EnableManagedSpotTraining=True|False,
##    CheckpointConfig={
##        'S3Uri': 'string',
##        'LocalPath': 'string'
##    },
##    DebugHookConfig={
##        'LocalPath': 'string',
##        'S3OutputPath': 'string',
##        'HookParameters': {
##            'string': 'string'
##        },
##        'CollectionConfigurations': [
##            {
##                'CollectionName': 'string',
##                'CollectionParameters': {
##                    'string': 'string'
##                }
##            },
##        ]
##    },
##    DebugRuleConfigurations=[
##        {
##            'RuleConfigurationName': 'string',
##            'LocalPath': 'string',
##            'S3OutputPath': 'string',
##            'RuleEvaluatorImage': 'string',
##            'InstanceType': 'ml.t3.medium'|'ml.t3.large'|'ml.t3.xlarge'|'ml.t3.2xlarge'|'ml.m4.xlarge'|'ml.m4.2xlarge'|'ml.m4.4xlarge'|'ml.m4.10xlarge'|'ml.m4.16xlarge'|'ml.c4.xlarge'|'ml.c4.2xlarge'|'ml.c4.4xlarge'|'ml.c4.8xlarge'|'ml.p2.xlarge'|'ml.p2.8xlarge'|'ml.p2.16xlarge'|'ml.p3.2xlarge'|'ml.p3.8xlarge'|'ml.p3.16xlarge'|'ml.c5.xlarge'|'ml.c5.2xlarge'|'ml.c5.4xlarge'|'ml.c5.9xlarge'|'ml.c5.18xlarge'|'ml.m5.large'|'ml.m5.xlarge'|'ml.m5.2xlarge'|'ml.m5.4xlarge'|'ml.m5.12xlarge'|'ml.m5.24xlarge'|'ml.r5.large'|'ml.r5.xlarge'|'ml.r5.2xlarge'|'ml.r5.4xlarge'|'ml.r5.8xlarge'|'ml.r5.12xlarge'|'ml.r5.16xlarge'|'ml.r5.24xlarge',
##            'VolumeSizeInGB': 123,
##            'RuleParameters': {
##                'string': 'string'
##            }
##        },
##    ],
##    TensorBoardOutputConfig={
##        'LocalPath': 'string',
##        'S3OutputPath': 'string'
##    },
##    ExperimentConfig={
##        'ExperimentName': 'string',
##        'TrialName': 'string',
##        'TrialComponentDisplayName': 'string'
##    },
##    ProfilerConfig={
##        'S3OutputPath': 'string',
##        'ProfilingIntervalInMilliseconds': 123,
##        'ProfilingParameters': {
##            'string': 'string'
##        }
##    },
##    ProfilerRuleConfigurations=[
##        {
##            'RuleConfigurationName': 'string',
##            'LocalPath': 'string',
##            'S3OutputPath': 'string',
##            'RuleEvaluatorImage': 'string',
##            'InstanceType': 'ml.t3.medium'|'ml.t3.large'|'ml.t3.xlarge'|'ml.t3.2xlarge'|'ml.m4.xlarge'|'ml.m4.2xlarge'|'ml.m4.4xlarge'|'ml.m4.10xlarge'|'ml.m4.16xlarge'|'ml.c4.xlarge'|'ml.c4.2xlarge'|'ml.c4.4xlarge'|'ml.c4.8xlarge'|'ml.p2.xlarge'|'ml.p2.8xlarge'|'ml.p2.16xlarge'|'ml.p3.2xlarge'|'ml.p3.8xlarge'|'ml.p3.16xlarge'|'ml.c5.xlarge'|'ml.c5.2xlarge'|'ml.c5.4xlarge'|'ml.c5.9xlarge'|'ml.c5.18xlarge'|'ml.m5.large'|'ml.m5.xlarge'|'ml.m5.2xlarge'|'ml.m5.4xlarge'|'ml.m5.12xlarge'|'ml.m5.24xlarge'|'ml.r5.large'|'ml.r5.xlarge'|'ml.r5.2xlarge'|'ml.r5.4xlarge'|'ml.r5.8xlarge'|'ml.r5.12xlarge'|'ml.r5.16xlarge'|'ml.r5.24xlarge',
##            'VolumeSizeInGB': 123,
##            'RuleParameters': {
##                'string': 'string'
##            }
##        },
##    ],
##    Environment={
##        'string': 'string'
##    }
##)
##


def preprocess(**kwargs):
    # import libraries
    #This code imports the required libraries and defines the environment variables you need to prepare the data, train the ML model, and deploy the ML model.
    import boto3, re, sys, math, json, os, sagemaker, urllib.request
    from sagemaker import get_execution_role
    import numpy as np
    import pandas as pd
##    from IPython.display import Image
##    from IPython.display import display
    from time import gmtime, strftime
    from sagemaker.predictor import csv_serializer

    #From Walter

    from airflow.contrib.operators.sagemaker_training_operator import SageMakerTrainingOperator
    # sagemaker sdk
    import sagemaker
    import boto3
    import botocore
    from sagemaker.processing import Processor
    # To define estimator
    from sagemaker.estimator import Estimator
    import io

    

##    # Define IAM role
##    role = get_execution_role()
##    prefix = 'sagemaker/DEMO-xgboost-dm'
##    containers = {'us-west-2': '433757028032.dkr.ecr.us-west-2.amazonaws.com/xgboost:latest',
##                  'us-east-1': '811284229777.dkr.ecr.us-east-1.amazonaws.com/xgboost:latest',
##                  'us-east-2': '825641698319.dkr.ecr.us-east-2.amazonaws.com/xgboost:latest',
##                  'eu-west-1': '685385470294.dkr.ecr.eu-west-1.amazonaws.com/xgboost:latest',
##                  'eu-west-2': '205493899709.dkr.ecr.eu-west-2.amazonaws.com/xgboost-neo:'} # each region has its XGBoost container
##    my_region = boto3.session.Session().region_name # set the region of the instance
##    print("Success - the MySageMakerInstance is in the " + my_region + " region. You will use the " + containers[my_region] + " container for your SageMaker endpoint.")
##

##    aws_session = SageMakerHook('aws_conn_id')
##    sagemaker_session = sagemaker.session.Session(aws_session.get_session())

    hook = AwsHook(aws_conn_id='aws_conn_id')
    sess = hook.get_session()
    
    log.info("Established successful Sagemaker instance")

    #loading data from S3 bucket

    #Establishing connection to S3 bucket
##    bucket_name = kwargs['bucket_name']
    #Establishing connection to S3 bucket
    bucket_name = 'ucl-msin0166-2021-london-housing-epc'
    key = 'output/dexters_epc_ratings.csv'
    s3 = S3Hook(kwargs['aws_conn_id'])
    log.info("Established connection to S3 bucket")

    # Read the content of the key from the bucket
    csv_final = s3.read_key(key, bucket_name)
    # Read the CSV
    clean_final_csv = pd.read_csv(io.StringIO(csv_final))#, encoding='utf-8')
    clean_final_csv
##    bucket = 'ucl-msin0166-2021-london-housing-epc'
##    subfolder = 'sagemaker'
##
##    from sagemaker import get_execution_role
##    role = get_execution_role()
##
##    conn = boto3.client('s3')
##
##    contents = conn.list_objects(Bucket=bucket, Prefix=subfolder)['Contents']
##
##    for f in contents:
##        print(f['Key'])
##
##        #loading data manually into diretory # figure out how to change this
##    try:
##        model_data = pd.read_csv('dexters_epc_ratings.csv',index_col=0)
##        print('Success: Data loaded into dataframe.')
##    except Exception as e:
##        print('Data load error: ',e)
##
    log.info("Data loaded successfully into dataframe")


# =============================================================================
# 3. Set up the main configurations of the dag
# =============================================================================
create_schema = PythonOperator(
    task_id='create_schema',
    python_callable=create_schema,
    op_kwargs=default_args,
    provide_context=True,
    dag=dag,
)

web_scraping_all_london_streets = PythonOperator(
    task_id='web_scraping_all_london_streets',
    python_callable=web_scraping_all_london_streets,
    op_kwargs=default_args,
    provide_context=True,
    dag=dag,
)

save_london_streets_to_postgres_db = PythonOperator(
    task_id='save_london_streets_to_postgres_db',
    provide_context=True,
    python_callable=save_london_streets_to_postgres_db,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,

)

get_sql_merged_table = PythonOperator(
    task_id='get_sql_merged_table',
    provide_context=True,
    python_callable=get_sql_merged_table,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,

)
##web_scraping_task_zoopla = PythonOperator(
##    task_id='web_scraping_function_zoopla',
##    provide_context=True,
##    python_callable=web_scraping_function_zoopla,
##    op_kwargs=default_args,
##    dag=dag,
##
##)
##
web_scraping_task_dexters = PythonOperator(
    task_id='web_scraping_task_dexters',
    provide_context=True,
    python_callable=web_scraping_function_dexters,
    op_kwargs=default_args,
    dag=dag,

)
##web_scraping_task_londonair = PythonOperator(
##    task_id='web_scraping_task_londonair',
##    provide_context=True,
##    python_callable=web_scraping_function_londonair,
##    op_kwargs=default_args,
##    dag=dag,
##
##)
##save_result_to_postgres_db_zoopla = PythonOperator(
##    task_id='save_result_to_postgres_db_zoopla',
##    provide_context=True,
##    python_callable=save_result_to_postgres_db_zoopla,
##    trigger_rule=TriggerRule.ALL_SUCCESS,
##    op_kwargs=default_args,
##    dag=dag,
##
##)
##
save_result_to_postgres_db_dexters = PythonOperator(
    task_id='save_result_to_postgres_db_dexters',
    provide_context=True,
    python_callable=save_result_to_postgres_db_dexters,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,

)

# preprocess the data
preprocess_task = PythonOperator(
    task_id='preprocess',
    dag=dag,
    provide_context=False,
    python_callable=preprocess, #name of the function that i have defined for preprocessing
    op_kwargs=default_args)



##
##prediction = SageMakerTrainingOperator(task_id='prediction',
##                                            trigger_rule=TriggerRule.ONE_SUCCESS,
##                                            dag=dag,
##                                            config=training_config(estimator=estimator, inputs=inputs), #estimator=xgb_model , inputs={"train": train_input, "validation": validation_input} # check the tutorials how to pass the inputs
##                                            wait_for_completion=True,
##                                            check_interval=30)
##

# =============================================================================
# 4. Indicating the order of the dags
# =============================================================================

create_schema >> web_scraping_all_london_streets >> save_london_streets_to_postgres_db >> get_sql_merged_table >> preprocess_task
create_schema >> web_scraping_task_dexters >> save_result_to_postgres_db_dexters >> get_sql_merged_table >> preprocess_task
##>> save_result_to_postgres_db_zoopla
##create_schema >> web_scraping_task_dexters >> save_result_to_postgres_db_dexters
##create_schema >> web_scraping_task_londonair >> save_Result_to_postgres_db_londonair

#For Alternative method
# create_schema >> web_scraping_task_dexters >> s3_save_file_func
