import boto3
import json
from datetime import datetime
import calendar
import random
import time
import sys
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API
consumer_key = 'PMLp3dNSTP3lR2QOQmbdQIpO4'
consumer_secret ='44S6QwmrFnxXidVvP4ZczKszOqr0LvgDJJh1h5RrfpDwRas5tQ'
access_token = '3312608045-p7UsMJKhnCujRcOg0kopJJ7DCdyueJ2fRFj9bdH'
access_token_secret = 'luUzRi21N3wFii12v4R5m5iYk3cjf6yTDYQW4VsCdgROH'


class TweetStreamListener(StreamListener):        
    # on success
    def on_data(self, data):
        # decode json
        tweet = json.loads(data)
        # print(tweet)
        if "text" in tweet.keys():
            payload = {'id': str(tweet['id']),
                                  'tweet': str(tweet['text'].encode('utf8', 'replace')),
                                  'ts': str(tweet['created_at']),
            },
            print(payload)
            try:
                put_response = kinesis_client.put_record(
                                StreamName=stream_name,
                                Data=json.dumps(payload),
                                PartitionKey=str(tweet['user']['screen_name']))
            except (AttributeError, Exception) as e:
                print (e)
                pass
        return True
        
    # on failure
    def on_error(self, status):
        print(status)


stream_name = 'twitter'  # fill the name of Kinesis data stream you created

if __name__ == '__main__':
    # create kinesis client connection
    kinesis_client = boto3.client('kinesis', 
                                  region_name='us-east-1',  # enter the region
                                  aws_access_key_id='AKIAV7IKULE7M2APVN4K',  # fill your AWS access key id
                                  aws_secret_access_key='luUzRi21N3wFii12v4R5m5iYk3cjf6yTDYQW4VsCdgROH')  # fill you aws secret access key
    # create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()
    # set twitter keys/tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    # create instance of the tweepy stream
    stream = Stream(auth, listener)
    
    
    query = sys.argv[1:] # list of CLI arguments
    query_fname = ' '.join(query) # string
    twitter_stream = Stream(auth, listener)
    twitter_stream.filter(track=query_fname, async=True)
    
    
    
    # search twitter for tags or keywords from cli parameters
    #query = sys.argv[1:] # list of CLI arguments 
    #query_fname = ' '.join(query) # string
    #print(query_fname)
    #print("hola")
    
    #try:
     #       stream.filter(track=query)
    #except Exception as e:
     #       print('Shutting down')
       #     print(e)
    