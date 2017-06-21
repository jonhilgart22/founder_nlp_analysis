#! usr/bin/env python
import twitter
import os
import yaml
import re
import time
import tweepy
import pandas as pd
from textblob import TextBlob
from collections import Counter
import pickle
import sys
__author__ = "Jonathan Hilgart"


def get_all_tweets(screen_name, api):
	"""Download the last 3240 tweets from a user. Do text processign to
    remove URLs and the retweets from a user.
	Adapted from https://gist.github.com/yanofsky/5436496"""
	# Twitter only allows access to a users most recent 3240 tweets with this method

	# initialize a list to hold all the tweepy Tweets
	alltweets = []
	# make initial request for most recent tweets (200 is the maximum allowed count)
	new_tweets = api.user_timeline(screen_name = screen_name,count=200)
	# save most recent tweets
	alltweets.extend(new_tweets)
	# save the id of the oldest tweet less one
	oldest = alltweets[-1].id - 1
	# keep grabbing tweets until there are no tweets left to grab
	while len(new_tweets) > 0:
		# all subsiquent requests use the max_id param to prevent duplicates
		new_tweets = api.user_timeline(screen_name = screen_name,count=200,max_id=oldest)
		# save most recent tweets
		alltweets.extend(new_tweets)

		#update the id of the oldest tweet less one
		oldest = alltweets[-1].id - 1

	print(f'Finished getting tweets for {screen_name}')
	cleaned_text = [re.sub(r'http[s]?:\/\/.*[\W]*', '', i.text,
                        flags=re.MULTILINE) for i in alltweets]  # remove urls
	cleaned_text = [re.sub(r'@[\w]*', '', i, flags=re.MULTILINE)
                 for i in cleaned_text] # remove the @twitter mentions
	cleaned_text = [re.sub(r'RT.*', '',  i, flags=re.MULTILINE)
                 for i in cleaned_text] # delete the retweets
	#transform the tweepy tweets into a 2D array that will populate the csv
	outtweets = [[tweet.id_str, tweet.created_at, cleaned_text[idx]]
              for idx,tweet in enumerate(alltweets)]


	return pd.DataFrame(outtweets,columns=["id","created_at","text"])


def scrub_text(tweets_df, return_bag_of_words = False, num_chars = 10):
    """Takes in a tweets DF and returns a list of sentences .
    Also creates the bag of words from the text to be used at a later time.
    num_chars specifies the minimum number of characters per sentence"""
    bag_of_words = None
    all_sentences = []
    for row in tweets_df.iterrows():
        blob = TextBlob(row[1]['text'])
        blob = blob.lower()
        blobl = blob.strip()# remove whitespace
        for sent in blob.sentences: ## append each sentence
            if len(sent) < num_chars: # sentences need to have at least ten characters
                pass
            else:
                all_sentences.append(str(sent)+" ")
        tokens = blob.tokenize()
        if bag_of_words == None:
            bag_of_words =  Counter(tokens)
        else:
            words = Counter(tokens)
            for k,v in words.items():
                if k in bag_of_words:
                    bag_of_words[k]+=v
                else:
                    bag_of_words[k]=v
    if return_bag_of_words == True:
        return all_sentences, bag_of_words
    else:
        return all_sentences


def save_founder_tweets(input_df, file_input, api):
    """For the entire dataframe, pull the twitter data and save the text
    to the raw data folder"""
    username_errors = []
    for idx, row in enumerate(input_df.iterrows()):
        total_rows = len(input_df)

        founder = row[1]['Primary Contact']
        company = row[1]['Company Name']
        twitter_username = row[1]['Twitter_Username']
        file_locations = ['vc_invest=0', 'vc_invest=1']
        if file_input not in file_locations:
            print(f"{file_input} is not a correct destination")
            break
        try:
            tweets = get_all_tweets(twitter_username, api )
            scrubbed_tweets = scrub_text(tweets)
            with open(f"../../data/raw/founders_tweets/{file_input}/{company}-{founder}-{twitter_username}",
                      "wb") as output_file:
                pickle.dump(scrubbed_tweets, output_file,
                            protocol=pickle.HIGHEST_PROTOCOL)
        except: # not authorized to see this user's timeline
            username_errors.append(founder) ## eventually drop these usernames

        print(f"{idx/total_rows:%} percent finished")




if __name__ == "__main__":
    # open credentials
    credentials = yaml.load(
        open(os.path.expanduser('~/.ssh/api_credentials.yml')))
    auth = tweepy.OAuthHandler(credentials['twitter']['consumer_key'],
                               credentials['twitter']['consumer_secret'])

    auth.set_access_token(credentials['twitter']['token'],
                       credentials['twitter']['token_secret'])

    twitter_api = tweepy.API(auth, wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True)
    # pass the file via the CLI to get the text from Twitter
    try:
        founder_company_df = pd.read_csv(str(sys.argv[1]))
    except:
        print('You entered the incorrect file path')
    file_input = sys.argv[2] # either vc_invest=1 or vc_invest=0
    # get all the text
    save_founder_tweets(founder_company_df, file_input, twitter_api)
