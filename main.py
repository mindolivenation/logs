"""
    Process stream logs
    @author mindosilalahi@livenation.com
"""
from __future__ import print_function
import json
import time
import boto3
from bson import json_util

CLIENT = boto3.client('logs')
def do_get(query, log_group_name, start_time, end_time):
    """
        To get cloudwatch records using insight
        :param: query str
        :param: log_group_name str
        :param: start_time int epoch
        :param: end_time int epoch
        :return: str
    """
    retry_counter = 0
    max_retry = 5

    start_query = CLIENT.start_query(
        logGroupName=log_group_name,
        startTime=start_time,
        endTime=end_time,
        queryString=query,
        limit=50
    )

    while retry_counter < max_retry:
        result = CLIENT.get_query_results(
            queryId=start_query.get('queryId')
        )

        if not result['results']:
            time.sleep(1)

        retry_counter += 1

    return json.dumps(result, default=json_util.default)

# usage
# log_group_name = '/aws/lambda/your-lambda-log'
# query = """
# fields @timestamp
# | filter @message like 'Foo: '
# | parse 'Foo: *' as @proc
# | sort @timestamp desc
# """
# start_time = 1559258543
# end_time = 1559344943
# do_get(query, log_group_name, start_time, end_time)
