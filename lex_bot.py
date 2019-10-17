import json
import datetime
import time 
import os
import dateutil.parser
import logging 
import boto3
from botocore.vendored import requests

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# --- Helpers that build all of the responses ---

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }

def close(session_attributes, fulfillment_state, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

def confirm_intent(session_attributes, intent_name, slots, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': intent_name,
            'slots': slots,
            'message': message
        }
    }
    
def build_validation_result(isvalid, violated_slot, message_content):
    return {
        'isValid': isvalid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }
    
# --- Helper Functions ---

def safe_int(n):
    """
    Safely convert n value to int.
    """
    if n is not None:
        return int(n)
    return n

def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False

def try_ex(func):
    """
    Call passed in function in try block. If KeyError is encountered return None.
    This function is intended to be used to safely access dictionary.

    Note that this function would have negative impact on performance.
    """

    try:
        return func()
    except KeyError:
        return None

# Function to validate input data
def validate_dining_input(slots):
    dining_date = try_ex(lambda: slots['Date'])
    dining_time = try_ex(lambda: slots['Time'])
    total_people = try_ex(lambda: slots['People'])
    contact_number = try_ex(lambda: slots['Phone'])
    
    # Validate total number of people
    if total_people and not (0 < safe_int(total_people) < 25):
        return build_validation_result(False,
                                        'People',
                                        'Total count exceeds 25 people. Please try again?')
    
    if dining_date:
        intended_date = dateutil.parser.parse(dining_date)
        grace_period = datetime.datetime.today() - datetime.timedelta(days=1)
        if intended_date < grace_period:
            return build_validation_result(
                False,
                'Date',
                'You cannot go back in time!'
            )
            
    # Validate Time
    if dining_date and dining_time:
        intended_time = dateutil.parser.parse(dining_date + ' ' + dining_time)
        if intended_time < datetime.datetime.now():
            return build_validation_result(False,
                                        'Time',
                                        'Unfortunately time machine does not exist!')
    
    # Validate phone number
    if contact_number and (contact_number.startswith('+1') is False or len(contact_number) != 12):
        return build_validation_result(
                False,
                'Phone',
                'Phone number must follow format +1XXXXXXXXXX'
            )
    
    return build_validation_result(True, None, None)

def dining_sqs(request):
    slots = request['currentIntent']['slots']
    location = try_ex(lambda: slots['Location'])
    cuisine = try_ex(lambda: slots['Cuisine'])
    dining_date = try_ex(lambda: slots['Date'])
    dining_time = try_ex(lambda: slots['Time'])
    total_people = try_ex(lambda: slots['People'])
    contact_number = try_ex(lambda: slots['Phone'])
    
    session_attributes = request['sessionAttributes'] if request['sessionAttributes'] is not None else {}
    
    # Load history and track current info
    reservation = json.dumps({
        'Location' : location,
        'Cuisine' : cuisine,
        'Date' : dining_date,
        'Time' : dining_time,
        'People' : total_people,
        'Phone' : contact_number 
    })
    
    session_attributes['currentReservation'] = reservation

    if request['invocationSource'] == 'DialogCodeHook':
        # Validate any slots which have been specified.  If any are invalid, re-elicit for their value
        validation_result = validate_dining_input(request['currentIntent']['slots'])
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(
                session_attributes,
                request['currentIntent']['name'],
                slots,
                validation_result['violatedSlot'],
                validation_result['message']
            )
    
        session_attributes['currentReservation'] = reservation
        return delegate(session_attributes, request['currentIntent']['slots'])
        
    sqs = boto3.client('sqs')
    queue_url = "https://sqs.us-east-1.amazonaws.com/040078798279/DiningQueue"
    
    response = sqs.send_message(
        QueueUrl=queue_url,
        DelaySeconds = 5,
        MessageAttributes={
            'Location': {'DataType': 'String',
                        'StringValue' : location
            },
            'Date': {
                'DataType': 'String',
                'StringValue': dining_date
            },
            'Time': {
                'DataType': 'String',
                'StringValue': dining_time
            },
            'People': {
                'DataType': 'String',
                'StringValue': total_people
            },
            'Cuisine': {
                'DataType': 'String',
                'StringValue': cuisine
            },
            'Phone': {
                'DataType': 'String',
                'StringValue': contact_number
            }
        },
        MessageBody=(
            'Suggest Restaurant'
        )
    )

    logger.debug("Inserted into queue. ID: %s" % response['MessageId'])

    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType' : 'PlainText',
            'content' : 'Done! Suggestions will arrive soon!'
        }
    )

def greetings(intent_request):
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return close(
        session_attributes,
        "Fulfilled",
        {
            'contentType': 'PlainText',
            'content': 'Hey there, how can I help?'
        }
    )

def thankyou(intent_request):
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Thanks for oredering, enjoy!'
        }
    )
# --- Intents ---

def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'DinningSuggestionsIntent':
        return dining_sqs(intent_request)
    elif intent_name == 'GreetingIntent':
        return greetings(intent_request)
    elif intent_name == 'ThankYouIntent':
        return thankyou(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')

# --- Main handler ---

def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)