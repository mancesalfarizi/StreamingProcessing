import tweepy
import time
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

#twitter setup
consumer_key = 'iM2FoJG91zMguV1xSrbRv66rf'         #api key sesuai akun twitter developer 
consumer_secret = 'MUZmk7OELyGrILOUv5oe6drn6MABf3vTIvHOWwSKf1ACfOunii'      #api key secret akun twitter developer
access_token = '1301040655670505472-cDZ7dFUnUc1QzPHgy9u7WlcKCKIdvI'     #access token sesuai akun twitter developer
access_token_secret = '2fD4hZglmOMQsErLmqlqnBpzRay8Oh5qO0ZnTZcuZ5SxF' #access token secret sesuai akun twit dev

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
	for i in (res):
		record = ''
		record += '{'
		record += '"'
		record += 'userId'
		record += '"'
		record += ':'
		record += str(i.user.id_str)
		record += ','
		record += '"'
		record += 'tanggalTweet'
		record += '"'
		record += ':'
		record += '"'
		record += str(normalize_timestamp(str(i.created_at)))
		record += '"'
		record += ','
		record += '"'
		record += 'isiTweet'
		record += '"'
		record += ':'
		record += '"'
		record += str(i.text)
		record += '"'
		record += ','
		record += '"'
		record += 'jumlahFollower'
		record += '"'
		record += ':'
		record += str(i.user.followers_count)
		record += ','
		record += '"'
		record += 'lokasi'
		record += '"'
		record += ':'
		record += '"'
		record += str(i.user.location)
		record += '"'
		record += ','
		record += '"'
		record += 'jumlahFavorite'
		record += '"'
		record += ':'
		record += str(i.favorite_count)
		record += ','
		record += '"'
		record += 'jumlahRetweet'
		record += '"'
		record += ':'
		record += str(i.retweet_count)
		record += '}'
		producer.send(topic_name, str.encode(record))

def periodic_work(interval):
	while True:
		get_twitter_data()
		#interval should be an integer, the number of seconds to wait
		time.sleep(interval)

periodic_work(60 * 0.1) # get data every couple of minutes