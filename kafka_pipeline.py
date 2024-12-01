from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime
import time

raw_topic_name = 'user-login'
processed_topic_name = 'processed_data'
bootstrap_servers = 'localhost:29092'

# Pull some insights:
processed_count = 0
error_count = 0
android_logins = 0
ios_logins = 0
start_time = datetime.now()


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
  print(f'Insights at - {datetime.now()}')
  print(f'Processed incoming messages: {processed_count}')
  print(f'Errored Messages: {error_count}')
  print(f'Android Logins: {android_logins}')
  print(f'iOS Logins: {ios_logins}')
  print(f'Over the past {time_span} seconds')

def format_data(value):
  global android_logins, ios_logins
  try:
    data = {}
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
      android_logins += 1
    if value['device_type'] == 'iOS':
      ios_logins += 1

    return data
  except KeyError as e:
    print(f'Error, missing data field: {e}')

print('Consuming message')

# Process messages:
for message in consumer:
  incoming_data = message.value

  print(f'Processing incoming message: {incoming_data}')

  processed_data = format_data(incoming_data)

  if processed_data:
    processed_count +=1
    print(f'Incoming message processed: {processed_data}')
    producer.send('processed_data', value=processed_data).add_callback(
    lambda metadata: print(f"Message sent to {metadata.topic}, partition {metadata.partition}")).add_errback(
    lambda err: print(f"Failed to send message: {err}")
    )
    producer.flush()
  else:
    error_count += 1
    print(f'Incoming message NOT processed: {incoming_data}')

# Run insights report every 30 seconds
  if processed_count % 30 == 0:
    insights()
