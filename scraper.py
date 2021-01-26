# modified from: https://github.com/gabrielpreda/Kaggle/blob/master/vaccination/vaccination-tweets.py
import os
import time
from tqdm import tqdm
import pandas as pd
import tweepy



def twitter_api():
	# Access twitter api
	print('[INFO] access twitter api')
	consumer_api_key = os.environ['TWITTER_CONSUMER_API_KEY']
	consumer_api_secret = os.environ['TWITTER_CONSUMER_API_SECRET']
	consumer_access_key = os.environ['TWITTER_CONSUMER_ACCESS_KEY']
	consumer_access_secret = os.environ['TWITTER_CONSUMER_ACCESS_SECRET']
	# Cereate and authenticate access
	auth = tweepy.OAuthHandler(consumer_api_key, consumer_api_secret)
	auth.set_access_token(consumer_access_key, consumer_access_secret)
	api = tweepy.API(auth, wait_on_rate_limit=True)
	return api


def create_cursor(api, search_words, date_since, date_until, language='id', items_limit=3000):
	# Create cursor to scrape tweets
	print('[INFO] create cursor')
	tweets = tweepy.Cursor(api.search,
						   q=search_words,
						   lang=language,
						   since=date_since,
						   until=date_until).items(items_limit)
	# Populate tweet_list with tweet from cursor
	print('[INFO] retreive new tweets')
	start_run = time.time()
	tweet_list = []
	for tweet in tqdm(tweets):
		tweet_list.append(tweet)
	end_run = time.time()
	print('no. of tweets scraped is {}'.format(len(tweet_list)))
	print('time take to complete is {}'.format(round(end_run-start_run, 2)))
	return tweet_list


def build_dataset(tweet_list):
	# Create dataframe from tweet_list
	print('[INFO] populating dataframe')
	tweets_df = pd.DataFrame()
	for tweet in tqdm(tweet_list):
		hashtags = []
		try:
			for hashtag in tweet.entities['hashtags']:
				hashtags.append(hashtag['text'])
		except:
			pass
		tweets_df = tweets_df.append(pd.DataFrame({
			'id': tweet.id,
			'date': tweet.created_at,
			'text': tweet.text, 
			'hashtags': [hashtags if hashtags else None],
			'user_name': tweet.user.name, 
			'user_location': tweet.user.location,
			'user_description': tweet.user.description,
			'user_created': tweet.user.created_at,
			'user_followers': tweet.user.followers_count,
			'user_friends': tweet.user.friends_count,
			'user_favourites': tweet.user.favourites_count,
			'user_verified': tweet.user.verified,
			'source': tweet.source,
			'retweets': tweet.retweet_count,
			'favorites': tweet.favorite_count,
			'is_retweet': tweet.retweeted,
			'reply_to_status':tweet.in_reply_to_status_id
		}, index=[0]))
	return tweets_df


def filter_dataframe(temp_df):
	# Filter foreign tweets using keywords
	df = temp_df.copy()
	text_filter = "malaysia|kuala lumpur|sabah|negeri sembilan|sarawak"
	f_text = df['text'].str.contains(text_filter, case=False, na=False)
	f_user_location = df['user_location'].str.contains(text_filter, case=False, na=False)
	f_user_description = df['user_description'].str.contains(text_filter, case=False, na=False)
	f = f_text|f_user_location|f_user_description
	return df[~f]


def update_dataset(new_df):
	# Update csv with new data
	file_path = 'indonesian_vaccination_tweets.csv'
	if os.path.exists(file_path):
		print('[INFO] update dataset')
		old_df = pd.read_csv(file_path)
		print('merge dataset...')
		print('old tweets: {}'.format(old_df.shape))
		join_df = pd.concat([old_df, new_df], axis=0)
		print('new tweets: {}'.format(new_df.shape))
		print('all tweets: {}'.format(join_df.shape))
		print('drop duplicate...')
		all_df = join_df.drop_duplicates(subset= ['id'], keep='last')
		print('drop non indonesian tweets...')
		all_df = filter_dataframe(all_df)
		print('final tweets: {}'.format(all_df.shape))
		all_df.to_csv(file_path, index=False)
	else:
		print('[INFO] create new dataset')
		print('tweets: {}'.format(new_df.shape))
		new_df.to_csv(file_path, index=False)


if __name__ == "__main__":
	api = twitter_api()
	search_words = '#vaksin OR #vaksinasi -filter:retweets'
	date_since = '2021-01-25'
	date_until = '2021-01-26'
	print('Search {0} since {1} until {2}'.format(search_words, date_since, date_until))
	# print('DATE {}'.format(date_since))
	tweet_list = create_cursor(api,
		search_words=search_words,
		date_since=date_since,
		date_until=date_until,
		language='id',
		items_limit=3000)
	tweets_df = build_dataset(tweet_list)
	update_dataset(tweets_df)
	print('DONE')
