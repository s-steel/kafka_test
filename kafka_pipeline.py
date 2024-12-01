from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime
import time

raw_topic_name = 'user-login'
processed_topic_name = 'processed_data'
bootstrap_servers = 'localhost:29092'

# Pull some insights:
PROCESSED_COUNT = 0
ERROR_COUNT = 0
ANDROID_LOGINS = 0
IOS_LOGINS = 0


# Consumer
consumer = KafkaConsumer(
    raw_topic_name, 
    bootstrap_servers=[bootstrap_servers],
    group_id='fetch-tech-challenge', 
    # auto_offset_reset='earliest'
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Producer
producer = KafkaProducer(
  bootstrap_servers=['localhost:29092'],
  value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def insights():
  time_span = (datetime.now() - start_time).seconds
  print(f'Insights - {datetime.now()}')
  print(f'Processed incoming messages: {PROCESSED_COUNT}')
  print(f'Errored Messages: {ERROR_COUNT}')
  print(f'Android Logins: {ANDROID_LOGINS}')
  print(f'iOS Logins: {IOS_LOGINS}')
  print(f'Over the past {time_span} seconds')

def format_data(key, value):
  try:
    data = {}
    data['key'] = key
    data['user_id'] = value['user_id']
    data['state'] = value['locale']
    data['app_version'] = value['app_version']
    # data['ip'] = value['ip'] # Don't need the ip address currently
    # data['device_id'] = value['device_id'] # don't need the device id
    data['device_type'] = value['device_type']
    dt_object = datetime.utcfromtimestamp(value['timestamp'])
    formatted_time = dt_object.strftime('%Y-%m-%d %H:%M:%S')
    data['timestamp'] = formatted_time

    if value['device_type'] == 'android':
      ANDROID_LOGINS += 1
    if value['device_type'] == 'iOS':
      IOS_LOGINS += 1

    return data
  except KeyError as e:
    print(f'Error, missing data field: {e}')

Print('Consuming message')

# Process messages:
for message in consumer:
  incoming_data = message.value
  key = message.key

  print(f'Processing incoming message: {incoming_data}')

  processed_data = format_data(key, incoming_data)

  if processed_data:
    PROCESSED_COUNT +=1
    print(f'Imcoming message processed: {processed_data}')
    producer.send(processed_topic_name, value=processed_data)
  else:
    ERROR_COUNT += 1
    print(f'Incoming message NOT processed: {incoming_data}')
    # Could improve message to more clearly explain issue.  eg/ missing a field

  if PROCESSED_COUNT % 10 == 0:
    insights()
