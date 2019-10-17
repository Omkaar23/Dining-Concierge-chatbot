from elasticsearch import Elasticsearch, RequestsHttpConnection
import boto3

# DynamoDB Configuration
ACCESS_KEY = 'AKIA2GICKLOIQ4CW5INM'
SECRET_KEY = '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'

client = boto3.resource('dynamodb',
                        aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY,
                        region_name='us-east-2'
                        )
table = client.Table('yelp-restaurants')

# ES Configuration
host = 'search-cloud-computing-jyu6tkfzpzykwmeuazyhtb6qd4.us-east-2.es.amazonaws.com'  # For example, my-test-domain.us-east-1.es.amazonaws.com

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

data = table.scan()

for item in data['Items']:
    item_dict = dict(item)
    final_dict = {}
    final_dict['RestaurantID'] = item_dict['business_id']
    final_dict['Cuisine'] = item_dict['cuisine']

    es.index(index="restaurants", id=item_dict['business_id'], body=final_dict)

print(es.get(index="restaurants", id='ykXZyQBXxoOMoZfaZMnHmg'))
