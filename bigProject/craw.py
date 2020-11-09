import tweepy
import time
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

#twitter setup
consumer_key = ''         #api key sesuai akun twitter developer 
consumer_secret = ''      #api key secret akun twitter developer
access_token = ''     #access token sesuai akun twitter developer
access_token_secret = '' #access token secret sesuai akun twit dev

#crating the authentication object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

# Setting your access token and secret
auth.set_access_token(access_token, access_token_secret)

# Creating the API object by passing in auth information
api = tweepy.API(auth)

def normalize_timestamp(time):
	mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
	return (mytime.strftime("%Y-%m-%d %H:%M:%S"))

producer = KafkaProducer(bootstrap_servers= 'localhost:9092')
topic_name = 'trump'

def get_twitter_data():
	res = api.search(" #nowplaying OR #listeningto OR #spotify" )
	for i in (res).items(1000):
		record = ''
		record += str(i.user.id_str)
		record += ';'
		record += str(normalize_timestamp(str(i.created_at)))
		record += ';'
		record += str(i.text)
		record += ';'
		record += str(i.user.followers_count)
		record += ';'
		record += str(i.user.location)
		record += ';'
		record += str(i.favorite_count)
		record += ';'
		record += str(i.retweet_count)
		record += ';'
		producer.send(topic_name, str.encode(record))

def periodic_work(interval):
	while True:
		get_twitter_data()
		#interval should be an integer, the number of seconds to wait
		time.sleep(interval)

periodic_work(60 * 0.1) # get data every couple of minutes