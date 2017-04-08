import requests
import configparser

config = configparser.ConfigParser()
config.read('credentials.ini')
api_user = config['GNIP_API']['username']
api_pass = config['GNIP_API']['password']

class GnipData():
    
    def __init__(self, maxResults, maxResultsPerPage, fromDate, toDate):
        self.maxResults = maxResults
        self.fromDate = fromDate
        self.toDate = toDate
        self.url = "https://gnip-api.twitter.com/search/" \
            "fullarchive/accounts/greg-students/prod.json"
        self.maxResultsPerPage = maxResultsPerPage
    
    def fetchTweets(self, query):
        extended_query = query+" has:geo place_country:us"
        params = {'query':extended_query, 
                  'maxResults':self.maxResultsPerPage,
                  'fromDate' : self.fromDate,
                  'toDate' : self.toDate
                 }
        response = requests.get(self.url, params=params, \
                         auth=(api_user, api_pass))

        self.queueKafka(response.json()['results'],-1 )

        for i in range(int(self.maxResults/self.maxResultsPerPage)-1):
            if 'next' in response.json().keys():
                params= {'query':extended_query, "next": response.json()['next']}
                response = requests.get(self.url, params=params, \
                             auth=(api_user, api_pass))
                self.queueKafka(response.json()['results'],i)
            else:
                break

    
    def queueKafka(self, json_data, i ):
        # queue json data in kafka
        print(i) #Remove this and the paremeter later.



if __name__ == "__main__":
    a = GnipData(100, 10 , "201612300000", "201612310000")
    a.fetchTweets("Apple")

