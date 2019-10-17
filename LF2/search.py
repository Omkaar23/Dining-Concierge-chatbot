import json
import boto3

# If running on lambda
# from botocore.vendored import requests

# Else
import requests



def get_message_from_sqs(response):
    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    input_message = message['Body']
    return receipt_handle, json.loads(input_message)


def parse_message_for_restaurant(restaurant):
    message = '\nRestaurant Name: ' + restaurant['name']
    message += '\nCuisine: ' + restaurant['cuisine']
    message += '\nRating: ' + str(restaurant['rating'])
    message += '\nReviews: ' + str(restaurant['review_count'])
    message += '\nAddress: ' + restaurant['address'] + ', ' + str(restaurant['zip'])
    return message


def lambda_function():
    # {"search":"indian","number":"+12015655705"}

    # SQS Configuration -> AWS Educate

    ACCESS_KEY_SQS = 'ASIAXCXNPOEGEK5K2ZUK'
    SECRET_KEY_SQS = '13O3ySvAt43oCmh27d5IwMKi841WFGMyW51+kni0'
    SESSION_TOKEN_SQS = 'FQoGZXIvYXdzEIL//////////wEaDBbLWppvCvRkQaViNSL9AbPJcl+kmLgR3QTbAKgrtoYMta0N1SKrLi+HqHBzMHFg5T66tYpQllszDDHtvkHInYcr79GpSQYKwSriwHG4xztZ6DTiHAU44E8HRo7/Lk7S59hqij03Uct8Fbr29cKUkMjkt0fXQ+5JztyBAYn5UMCbyNHqvPoIDtn5+3dg2UrpqwY2+vfmu8NxUv+A24DSkFw+UHj2FDQ0sHL60RP/VW46sxc0Xjp6i3HqZOBDJUpUut9cgqr1AJykoo3/cTzMSPUpYwcUOUSJBubcbPS64LKv2oFj336zwe8asUOT9V87U6n/bhE1/r46nDROpMAtx943fWFysKgr7YKEPXoooPCe7QU='
    SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/486903083276/search_queue'

    sqs = boto3.client('sqs',
                       aws_access_key_id=ACCESS_KEY_SQS,
                       aws_secret_access_key=SECRET_KEY_SQS,
                       aws_session_token=SESSION_TOKEN_SQS
                       )

    # SNS Configuration -> Free Tier

    ACCESS_KEY_SNS = 'AKIAZMNJVB4FFMAL72JH'
    SECRET_KEY_SNS = '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'

    sns = boto3.client('sns',
                       aws_access_key_id=ACCESS_KEY_SNS,
                       aws_secret_access_key=SECRET_KEY_SNS
                       )

    # DynamoDB Configuration -> Omkaar

    ACCESS_KEY_DYNAMO = 'AKIA2GICKLOIQ4CW5INM'
    SECRET_KEY_DYNAMO = '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'
    TABLE_NAME = 'yelp_restaurants'

    dynamodb = boto3.resource('dynamodb',
                              aws_access_key_id=ACCESS_KEY_DYNAMO,
                              aws_secret_access_key=SECRET_KEY_DYNAMO,
                              region_name='us-east-2'
                              )
    table = dynamodb.Table(TABLE_NAME)

    # ES Configuration -> Open (but hosted @ Free Tier)
    ES_HOST = 'https://search-cloud-computing-jyu6tkfzpzykwmeuazyhtb6qd4.us-east-2.es.amazonaws.com'
    ES_SEARCH_PATH = '/restaurants/_search?q='

    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL
    )
    if 'Messages' in response.keys():

        final_message = 'Here Are a few suggestions according to your recent request:'
        final_message += '\n*********************'

        receipt_handle, input_dict = get_message_from_sqs(response)

        # Search from ES
        r = requests.get(ES_HOST + ES_SEARCH_PATH + input_dict['search'])
        if r.status_code == 200:
            response = json.loads(r.text)

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
                PhoneNumber=input_dict['number'],
                Message=final_message
            )

            # Delete received message from queue
            sqs.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )
        else:
            return 'Error'

    else:
        return 'Error'
