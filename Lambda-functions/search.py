import json
import boto3
import requests

def get_message_from_sqs(response):
    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    input_message = message['MessageAttributes']
    return receipt_handle, input_message

def parse_message_for_restaurant(restaurant):
    message = '\nRestaurant Name: ' + restaurant['name']
    message += '\nCuisine: ' + restaurant['cuisine']
    message += '\nRating: ' + str(restaurant['rating'])
    message += '\nReviews: ' + str(restaurant['review_count'])
    message += '\nAddress: ' + restaurant['address'] + ', ' + str(restaurant['zip'])
    return message
    
def lambda_handler(event, context):
    
    # SQS Configuration
    SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/040078798279/DiningQueue'
    sqs = boto3.client('sqs')

    # SNS Configuration
    sns = boto3.client('sns')

    # DynamoDB Configuration -> Omkaar
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp_restaurants')

    # ES Configuration -> Open (but @ Free Tier)
    ES_HOST = 'https://search-cloud-computing-jyu6tkfzpzykwmeuazyhtb6qd4.us-east-2.es.amazonaws.com'
    ES_SEARCH_PATH = '/restaurants/_search?q='

    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ]
    )
    if 'Messages' in response.keys():

        receipt_handle, input_dict = get_message_from_sqs(response)

        cuisine = input_dict['Cuisine']['StringValue']
        phone_number = input_dict['Phone']['StringValue']

        # Search from ES
        r = requests.get(ES_HOST + ES_SEARCH_PATH + cuisine)
        if r.status_code == 200:
            response = json.loads(r.text)
            if len(response['hits']['hits']) == 0:
                final_message = "Hey There! Sorry, we couldn't find any results for {}. " \
                                "While we keep adding restaurants, try searching with another keyword".format(cuisine)
            else:

                final_message = 'Hey! Thank You for using our service :D\n' \
                                'In the mood for {} ? \n' \
                                'Here are a few restaurants we suggest: \n' \
                                '*********************'.format(cuisine)
                # Iterate Over Every Suggestion and compute response message
                for restaurant in response['hits']['hits']:
                    restaurant_details = table.get_item(
                        Key={
                            'business_id': restaurant['_id']
                        }
                    )['Item']
                    restaurant_message = parse_message_for_restaurant(restaurant_details)

                    final_message += restaurant_message
                    final_message += '\n*********************'

            # Send your sms message.
            sns.publish(
                PhoneNumber=phone_number,
                Message=final_message
            )

            # Delete received message from queue
            sqs.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )

            return {
                'statusCode': 200,
                'body': json.dumps('Successful!')
            }
        else:
            sns.publish(
                PhoneNumber=phone_number,
                Message="Sorry, Something went wrong, we're working on it. Please Try again after sometime :)"
            )
            # Delete received message from queue
            sqs.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )

            return {
                'statusCode': 500,
                'body': json.dumps('OOPS, Something went wrong!')
            }
