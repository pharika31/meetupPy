'''
Created on Oct 2, 2017

@author: harik
'''
from kafka import SimpleProducer, KafkaClient
from websocket import create_connection
kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)

def rsvp_source():
    """
    1. Connect to meetup rsvp websocket stream and get live rsvp data as json.
    2. Receives the stream.
    3. Sends the stream as a producer.
    """
    ws = create_connection('ws://stream.meetup.com/2/rsvps')
    while True:
        try:
            rsvp_data = ws.recv() # Meetup API provides websocket stream to .gGet real-time data.
            if rsvp_data:
                # Send the stream to the topic "meetup_stream". kafka topic is "meetup_stream"
                producer.send_messages("meetup_stream", rsvp_data)
                
                print(rsvp_data)
        # Call the function recursively despite of any exceptions.
        except Exception as e:
            print(e);
            rsvp_source()

if __name__ == '__main__':
    rsvp_source()