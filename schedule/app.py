import json
from logging import error
import boto3
import os
import requests
from botocore.exceptions import ClientError
import datetime
import pytz
import random

# get env variables
table_name =os.getenv('dynamoDbTable')
s3_bucket = os.getenv('s3Bucket')
region = os.getenv('awsRegion')
secret_name = os.getenv('secretName')

# get secret from ssm
def get_secret():
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager',region_name=region)
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    else:
        return get_secret_value_response
        

# convert local time to UTC
def convert_to_UTC(timezone, date_time):
    local_timezone = pytz.timezone(timezone)
    date_object = datetime.datetime.strptime(date_time, "%y/%m/%d %H:%M:%S")
    local_datetime = local_timezone.localize(date_object, is_dst=None)
    utc_datetime = local_datetime.astimezone(pytz.utc)
    return utc_datetime

# scan object from dynamodb 
def scan_objects():
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    try:
        dynamo_object = table.scan()
    except Exception as e:
        print(e)
        raise e
    else:
        return dynamo_object

# write file to s3
def write_to_s3(json_object, file_name, token):
    s3 = boto3.resource('s3')
    object = s3.Object(s3_bucket, "%s.json"%file_name)
    s3_object = json.dumps({"Header": {"Authorization":token},"Body": json_object}, default=str)
    try:
        result = object.put(Body=s3_object)
    except Exception as e:
        print(e)
        raise e
    else:
        print(result)

# Find time difference
def timeDiff(t1, t2):
    FMT = '%H:%M'
    time_difference = datetime.datetime.strptime(t2, FMT) - datetime.datetime.strptime(t1, FMT)
    return time_difference

# calculate the total intervals and randomize the last schedule
def randomize(intervals):
    randomize_interval = intervals[len(intervals) - 1]
    random_number = randomize_interval + random.randint(0, 600)
    intervals[len(intervals) - 1] = random_number
    return intervals

# calculate end time
def end_time(date_time, time_to_add):
    hours_added = datetime.timedelta(hours = time_to_add)
    UTC_end_time = date_time + hours_added
    return UTC_end_time    

# extract hours and minutes from the dateTime
def extract_hours_minutes(date_time):
    hour = str(date_time.hour)
    minute = str(date_time.minute)
    zero_filled_hour = hour.zfill(2)
    zero_filled_minute = minute.zfill(2)
    zero_filled_time = zero_filled_hour+':'+zero_filled_minute
    return zero_filled_time

# add hours into dateTime
def add_hours(hours):
    return datetime.timedelta(hours = hours)


# Main Lamdba function
def lambda_handler(event, context):
    # parse local_dateTime, 
    # type, 
    # devId, 
    # startAt, 
    # interval, 
    # and maxWh from request body
    event = json.loads(event['body'])     
    type = event['type']
    dev_id = event['devId']
    start_at = event['startAt']
    interval = event['interval']
    maxWh = event['maxWh']

    # check token 
    secret = get_secret()
    params = json.loads(secret['SecretString'])
    print(params)
    token = params['fakeToken']
    if token:
        # call the scan function and return records
        response = scan_objects()
        # parse device_id, 
        # timezone, 
        # local_start_opt_out, 
        # local_end_opt_out 
        # from the dynamodb object
        device_id = response['Items'][0]['device_id']
        timezone = response['Items'][0]['timezone']
        local_start_opt_out = response['Items'][0]['local_start_opt_out']
        local_end_opt_out = response['Items'][0]['local_end_opt_out']
        # convert localtime to UTC
        UTC_start_time = convert_to_UTC(timezone, start_at)
        # sum all intervals and calculate utc_end_time
        hours = 0
        for i in interval:
            hours = hours + i
        hours = hours / 60 /60
        hours_added = add_hours(hours)
        UTC_end_time = UTC_start_time + hours_added
        print(UTC_start_time)
        # calculate intervals with last random schedule
        intervals = randomize(interval)

        #extract hours and minutes from the start and end dateTime
        extract_hours_minutes_start = extract_hours_minutes(UTC_start_time)
        print(extract_hours_minutes_start)
        extract_hours_minutes_end = extract_hours_minutes(UTC_end_time)
        print(extract_hours_minutes_end)
        #exclude if opt_out_time in actual time range
        if(extract_hours_minutes_start <= local_end_opt_out and extract_hours_minutes_end >= local_start_opt_out):
            return{
                "statusCode": 200,
                "body": json.dumps({"message": "This device is opt out"})
            }
        else: 
            # create response payload
            result = {
                "type" : type,
                "devId" : dev_id,
                "startAt": UTC_start_time.isoformat(),
                "interval": intervals,
                "maxWh": maxWh
            }
            # write response to s3 file
            write_to_s3(result, device_id, token)
            resp = requests.post(
                'https://hooks.slack.com/services/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', 
                json={"text":"Schedule for device %s"%device_id}, 
                headers = {'Content-Type': 'application/json'}
            )
            print(resp)
            # s3_reponse = write_to_s3(result, device_id)
            # print(s3_reponse)
            return {
                "statusCode": 200,
                "body": json.dumps({"message": result})
            }
