'''
Created on Oct 4, 2017

@author: harik
'''
from kafka import KafkaConsumer

consumer = KafkaConsumer('meetup_stream', group_id = '1', bootstrap_servers = ['localhost:9092'])

for message in consumer:
    try: 
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
    except Exception as e:
        print(e)