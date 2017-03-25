import requests
import json
import configparser
from pykafka import KafkaClient

##parameters for the search
url ="https://gnip-api.twitter.com/search/fullarchive/accounts/greg-students/prod.json"
parameters = { 'query':'gnip has:profile_geo', 'maxResults':'10',
'fromDate':'201701010000', 'toDate':'201701050000'}

##reading credentails file
config = configparser.ConfigParser()
config.read('credentials.ini')

##connecting to the kafka client
client = KafkaClient("localhost:9092")

##selcting the topic
topic = client.topics["test"]

##writing into topic
with topic.get_producer() as producer:
       producer.produce(bytes(requests.get(url,
           auth=(config['GNIP_API']['username'],
               config['GNIP_API']['password']),
           params=parameters).json()['results']));
