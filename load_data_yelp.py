
#
#  Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  This file is licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License. A copy of
#  the License is located at
# 
#  http://aws.amazon.com/apache2.0/
# 
#  This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
#  CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
#
from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import datetime

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('yelp_restaurants')
coordinates = []
with open("data_yelp_final.json") as json_file:
    yelp_data = json.load(json_file)
    for record in yelp_data:
        business_id = record['id']
        name = record['name']
        address = record['location']['address1']
        zip_code = record['location']['zip_code']
        coordinates.append(int(record['coordinates']['latitude']))
        coordinates.append(int(record['coordinates']['longitude']))
        rating = int(record['rating'])
        num_count = record['review_count']
        cuisine = record['categories'][0]['title']
        ts = datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")

        if not zip_code:
            zip_code = ' '
        if not name:
            name = ' '   
        if not cuisine:
            cuisine = ' '
        if not address:
            address = ' '                  
        
        table.put_item(Item={
                    
                    'business_id': business_id,
                    'name': name,
                    'cuisine': cuisine,
                    'address': address,
                    'coordinates': coordinates,
                    'rating': rating,
                    'review_count': num_count,
                    'zip': zip_code,
                    'insertedAtTimestamp': ts,

                }
            )
        
        coordinates.clear()
