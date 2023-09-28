import sys
import boto3
import botocore.exceptions
import requests
import urllib3
import ssl
from logger import logger
# from secrets_access import get_secret
import snowflake.connector
# from snowflake.snowpark import Session
import os
from dotenv import load_dotenv, find_dotenv

# Get environment vars
load_dotenv(find_dotenv())

# Set logger
logger = logger('connector')


def get_snowflake_connection(method):
    """ Confirm Access to Snowflake"""
    try:
        params = {
            "user": os.getenv('SFUSER'),
            "password": os.getenv('SFPW'),
            "account": os.getenv('SNOWFLAKE_ACCT'),
            "warehouse": os.getenv('SFWH'),
            "database": os.getenv('SNOWFLAKE_DB'),
            "schema": os.getenv('SFSCHEMA')
        }

        if method == 'standard':
            conn = snowflake.connector.connect(
                user=params['user'],
                password=params['password'],
                account=params['account'],
                warehouse=params['warehouse'],
                database=params['database'],
                schema=params['schema']
            )

            return conn

        # if method == 'spark':
        #     session = Session.builder.configs(params).create()
        #     return session

    except Exception as e:
        raise e


def s3_conn(func):
    """ Confirm Access to S3 """
    def wrapper_s3_checks(*args, **kwargs):
        """ Provides a series of checks for S3 before running a function provided """

        s3_client, s3_resource = boto3.client('s3'), boto3.resource('s3')
        logger.info('Successfully connected to S3.')

        # Stores the passed args and kwargs into available lists
        arg_vars, kw_vars = [arg for arg in args], [arg for arg in kwargs]

        try:
            if type(func) != str:
                pass
            else:
                # Check if the bucket exists. If a bucket exists, it should be passed into a function as an argument.
                buckets = [name['Name'] for name in s3_client.list_buckets()['Buckets']]
                matches = [name for name in arg_vars if name in buckets]
                if not matches:
                    logger.error(f'S3 Bucket provided does not exist: {arg_vars}')

            return func(*args, **kwargs)

        except Exception as e:
            logger.error(f"Something went wrong: {e}")
            sys.exit(-1)

    return wrapper_s3_checks


class CustomHttpAdapter (requests.adapters.HTTPAdapter):
    # "Transport adapter" that allows us to use custom ssl_context.

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, ssl_context=self.ssl_context)


def get_legacy_session():
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    session = requests.session()
    session.mount('https://', CustomHttpAdapter(ctx))
    return session
