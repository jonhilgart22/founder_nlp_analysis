# -*- coding: utf-8 -*-
#!/usr/bin/python
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
from PitchBook_make_VCInvest_one_data import read_in_data, drop_cols, impute_median_values, get_twitter_usernames
from pull_twitter_text import get_all_tweets, scrub_text, save_founder_tweets
from add_Watson_nlp_features import add_nlp_features, aggregated_tone_analyzer

if __name__ == '__main__':
    ######### STEP ONE VC invest = 1 ############
    # Create the .csv for VC =1
    initial_df = read_in_data()
    dropped_df = drop_cols(initial_df)
    imputed_df = impute_median_values(dropped_df)
    add_username_df = get_twitter_usernames(imputed_df)
    add_username_df.to_csv("../../data/processed/PitchBook_CA_VCInvest=1.csv")
    ########### STEP TWO  VC Invest =1##############
    # Get and save the tweets from the founders
    credentials = yaml.load(
        open(os.path.expanduser('~/.ssh/api_credentials.yml')))
    auth = tweepy.OAuthHandler(credentials['twitter']['consumer_key'],
                               credentials['twitter']['consumer_secret'])

    auth.set_access_token(credentials['twitter']['token'],
                       credentials['twitter']['token_secret'])

    twitter_api = tweepy.API(auth, wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True)
    try:
        founder_company_df = pd.read_csv(
            "../../data/processed/PitchBook_CA_VCInvest=1.csv")
    except:
        print('You entered the incorrect file path')
    file_input = 'vc_invest=1' # either vc_invest=1 or vc_invest=0
    # get all the text and save it
    save_founder_tweets(founder_company_df, file_input, twitter_api)
    ######### STEP THREE  VC INvest =1 #############
    # Add the watson NLP features
    # load in data

    #"../../data/processed/PitchBook_CA_VCInvest=1.csv"
    #../../data/processed/PitchBook_CA_VCInvest=0.csv
    pitchbook_df = pd.read_csv("../../data/processed/PitchBook_CA_VCInvest=1.csv")
    final_df = add_nlp_features(pitchbook_df, vc_type=1) # add vc type here
    # save csv
    # "../../data/processed/PitchBook_CA_VCInvest=1_NLP-features.csv"
    #../../data/processed/PitchBook_CA_VCInvest=0_NLP-features.csv
    final_df.to_csv("../../data/processed/PitchBook_CA_VCInvest=1_NLP-features.csv")
