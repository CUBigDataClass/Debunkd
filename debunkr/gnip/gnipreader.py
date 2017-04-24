import requests
import configparser
import json
from kafka import KafkaProducer

#We need better storage for all of these
#----------------------------------------
config = configparser.ConfigParser()
config.read('credentials.ini')
api_user = config['GNIP_API']['username']
api_passwd = config['GNIP_API']['password']
TOPIC_NAME= "gnipstream"
KAFKA_ADDRESS = "172.32.13.183:32777"
#----------------------------------------


class GnipData():
    """
    Gets data from Gnip and pushes it to a Kafka queue.
    Params-
    - maxResults (int): How many Results you want
    - maxResultsPerPage (int): Results per page, to get further we use the next param
    - fromDate (string- yyyymmddhhmmss) : Starting Date of all the tweets returned
    - toDate (string -yyyymmddhhmmss): Ending Date of all the tweets returned
    """
    def set_up(self):
        #self.TOPIC_NAME= "test4"
        #config = configparser.ConfigParser()
        #config.read('credentials.ini')
        self.url = "https://gnip-api.twitter.com/search/" \
            "fullarchive/accounts/greg-students/prod.json"
        #self.api_user = config['GNIP_API']['username']
        #self.api_passwd = config['GNIP_API']['password']
        self.kafka_server = KAFKA_ADDRESS
        self.kafka_producer = KafkaProducer(bootstrap_servers = [self.kafka_server])
        #this works in jupyter but not in terminal... working around
        # self.kafka_producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('unicode'),
        #                                    bootstrap_servers=[self.kafka_server])


    def __init__(self, fromDate, toDate):
        self.fromDate = fromDate
        self.toDate = toDate        
        self.set_up()

    def fetchTweets(self, query):
        """
        Takes a query and pushes the relevant tweets to Kafka
        Params
        - Query: Search term for Gnip

        Returns : Nothing
        """
        extended_query = query+" has:geo place_country:us"
        params = {'query':extended_query,
                  'maxResults': 500,
                  'fromDate' : self.fromDate,
                  'toDate' : self.toDate
                 }
        response = requests.get(self.url, params=params, \
                         auth=(api_user, api_passwd))

        for r in response.json()['results']:
        	    r['topic']= Query
                self.queueKafka( json.dumps(r).encode('utf-8'))

        #Scrolling through until next runs out or maxResults is exceeded
        while 'next' in response.json().keys():
            params= {'query':extended_query, "next": response.json()['next']}
            response = requests.get(self.url, params=params, \
                         auth=(api_user, api_passwd))

            for r in response.json()['results']:
            		r['topic']= query
                    self.queueKafka( json.dumps(r).encode('utf-8'))
            #self.queueKafka(json.dumps(response.json()['results']).encode('utf-8'))
        


    def queueKafka(self, json_data ):
        """
        Queues json to Kafka
        Params
        - json_data: Json data to queue in Kafka

        Returns : Nothing
        """
        self.kafka_producer.send(TOPIC_NAME, json_data)


if __name__ == "__main__":
    a = GnipData( "201612300000", "201612310000")
    a.fetchTweets("hillary")
