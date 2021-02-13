import multiprocessing as mp
import time
import random, string
import pika
import json
import uuid 
from random import randrange
import requests
from config import settings

QUEUE = 'parallelagram'
DLQ = 'parallelagram_DLQ'
MANUAL_DLQ = 'parallelagram_manual_work'
DLQ_LIMIT = 2
PRODUCER_TIMEOUT = .1
MOCK_API_SPEED = .2

SETTINGS = settings()

def _connect_to_rabbit():
    credentials = pika.PlainCredentials(SETTINGS['username'], SETTINGS['password'])
    connection = pika.BlockingConnection(pika.ConnectionParameters(SETTINGS['host'],SETTINGS['port'], '/', credentials))
    channel = connection.channel()

    #channel.queue_declare(queue=QUEUE)
    return channel


def _queue_callback(channel, method, properties, body):
    #print(f"Received message: {body.decode('UTF-8')}")

    record_checklist(channel, body.decode('UTF-8'))

def record_checklist(channel, basic_record):
    '''
        Initiate Checklist for consumer to build out record
    '''
    #print("initiating checklist")

    record = json.loads(basic_record)

    try:
        record['favorite'] = enrich_data(record['id'])
    except requests.exceptions.Timeout as e:
        ''' Move to DLQ '''
        print(f"DLQ Warning - Record Id: {record['id']}")
        _push_to_dlq(channel, DLQ, record)
    except Exception as e:
        ''' some other error, will need to handle this '''
        print(e)

    print(record)
    
def enrich_data(id):
    # Mock call to api by id

    #Api returns enriched sub-object
    obj = {}
    obj['favorite_record_id'] = ''.join(random.choice(string.digits) for _ in range(9))
    obj['favorite_type_id']  = ''.join(random.choice(string.digits) for _ in range(9))
    obj['favorite_api_id'] = str(uuid.uuid4())
    time.sleep(MOCK_API_SPEED)

    # Random timeout error to test DLQ
    if randrange(0, 9) == 5:
        raise requests.exceptions.Timeout

    return json.dumps(obj)

def _push_to_dlq(channel, dlq, obj):
    ''''''
    

    dlq_count = obj.get('dlq_count', None)
    if dlq_count == None:
        obj['dlq_count'] = 1
    else:
        current_count = obj['dlq_count']
        current_count += 1
        obj['dlq_count'] = current_count

        message = json.dumps(obj)
        if current_count >= DLQ_LIMIT:
            channel.queue_declare(queue=MANUAL_DLQ)
            channel.basic_publish(exchange="", routing_key=dlq, body=message)
            return 
    
    message = json.dumps(obj)
    channel.queue_declare(queue=dlq)
    channel.basic_publish(exchange="", routing_key=dlq, body=message)
    print(f"Pushed onto the DLQ: {message}")




def produce():
    ''''''
    channel = _connect_to_rabbit()
    while True:
        record = make_record()
        message = json.dumps(record)
        channel.basic_publish(exchange="", routing_key=QUEUE, body=message)
        #print(f"Produced message: {message}")
        time.sleep(PRODUCER_TIMEOUT)

def make_record():
    record = {}
    record['id'] = str(uuid.uuid4())
    record['first_name'] = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
    record['last_name'] = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(15))  
    return record 


def consume():
    ''''''
    channel = _connect_to_rabbit()
    # channel.basic_consume(queue=QUEUE, on_message_callback=_queue_callback, auto_ack=True)
    # channel.start_consuming()
    while True:
        method_frame, header_frame, body = channel.basic_get(queue=QUEUE)
        if method_frame:
            # print(method_frame, header_frame, body)
            # print(body.)
            channel.basic_ack(method_frame.delivery_tag)
            _queue_callback(channel, None, None, body)

        dlq_frame, _, dlq_body = channel.basic_get(queue=DLQ)
        if dlq_frame:
            channel.basic_ack(dlq_frame.delivery_tag)
            _queue_callback(channel, None, None, dlq_body)
  



if __name__ == '__main__':
    p1 = mp.Process(name='p1', target=produce)
    p2 = mp.Process(name='p2', target=consume)
    p3 = mp.Process(name='p3', target=consume)
    p4 = mp.Process(name='p4', target=consume)
    p5 = mp.Process(name='p5', target=consume)
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()