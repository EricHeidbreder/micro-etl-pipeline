#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#

import boto3
import logging
import pandas as pd
import os
import requests
import json
import datetime

# Environment variables
URL = os.environ['Url']
S3_BUCKET = os.environ['S3Bucket']
LOG_LEVEL = os.environ['LogLevel']
FILENAME = os.environ['Filename'].split('.')[0] + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + '.csv'

# Log settings
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

# Lambda function handler
def lambda_handler(event, context):
    logger.info('## EVENT')
    logger.info(event)
    
    # columns = ['Date', 'Region_Name', 'Area_Code', 'Detached_Average_Price',
    #    'Detached_Index', 'Detached_Monthly_Change', 'Detached_Annual_Change',
    #    'Semi_Detached_Average_Price', 'Semi_Detached_Index',
    #    'Semi_Detached_Monthly_Change', 'Semi_Detached_Annual_Change',
    #    'Terraced_Average_Price', 'Terraced_Index', 'Terraced_Monthly_Change',
    #    'Terraced_Annual_Change', 'Flat_Average_Price', 'Flat_Index',
    #    'Flat_Monthly_Change', 'Flat_Annual_Change']
    
    # Request to get the last 2000000 bytes to get the most recent data in the CSV skipping the first row
    # Implies that the value are in ascending order with most recent at the end of the file
    res = requests.get(URL)

    if res.status_code == 200:
        data = json.loads(res.text)
    else:
        logger.info(f'## ERROR | Status Code: {res.status_code}')
    data_per_record = list(map(lambda x: {**x['geometry'], **x['properties']}, data['features'])) # merge the geometry and properties objects of each feature
    morel_df = pd.DataFrame(data_per_record)
    logger.info('## NUMBER OF ELEMENTS')
    logger.info(morel_df.size)

    # Save files into S3
    boto3.resource('s3').Bucket(S3_BUCKET).put_object(Key=FILENAME, Body=morel_df.to_csv(index=False))
    url = 's3://{}/{}'.format(S3_BUCKET, FILENAME)
    # morel_df.to_csv(url)
    logger.info('## FILE PATH')
    logger.info(url)