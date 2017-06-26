#! usr/bin/end python
import pandas as pd
import pickle
import requests
import json
import os
import yaml
from watson_developer_cloud import DiscoveryV1, ToneAnalyzerV3, PersonalityInsightsV3
from collections import defaultdict
import sys
__author__ = 'Jonathan Hilgart'

def aggregated_tone_analyzer(sentences_list, n_sentences=250):
    """Takes in a list of sentences and and outputs the average Emotioinal
    Tones (anger, disgust, fear, joy, sadness),
    Social Tones (openess, conscientiousness, extraversion, agreeableness, emotional range)
    across all of the input sentences.

    Returns a dictionary of emotional_tones, language_tones, then social tones.

    Assumes you have imported the ToneAnalyzerV3 from the Watson APIs.

    Feeds in  n_sentences at a time to the Watson API"""
    # Authenticate the Watson API
    credentials_tone = yaml.load(open(os.path.expanduser('~/.ssh/watson_cred.yml')))
    tone_analyze = ToneAnalyzerV3(
        username=credentials_tone['ToneAnalyzerV3']['username'],
        password=credentials_tone['ToneAnalyzerV3']['password'],
        version="2016-12-01")
    # store the results
    emotional_tones = defaultdict(int)
    language_tones = defaultdict(int)
    social_tones = defaultdict(int)
    total_num_sentences = len(sentences_list)
    print(f"Total number of sentences to be analyzed = {total_num_sentences }")
    # store the chuncks of sentences
    sentences_chunk = ""
    number_of_chunks = 0
    for idx,sent in enumerate(sentences_list):
        sent = sent.replace("\ud83d","").replace("\n","").replace("\t","") # remove bad JSON
        sentences_chunk+=str(sent)

        if (idx % n_sentences == 0)  or (idx +1 == total_num_sentences ) and (idx!=0): # perform analysis
            #print('PERFORM ANALYSIS')
            #print()
            try:
                response = tone_analyze.tone(sentences_chunk)

                # append the score for emotional tones
                for emot_tone in response['document_tone']['tone_categories'][0]['tones']:
                    # 0 is emotion , 1 is language 2 = social
                    emotional_tones[emot_tone['tone_id']]+= emot_tone['score']
                # append the scores for the language tones
                for lang_tone in response['document_tone']['tone_categories'][1]['tones']:
                    # 0 is emotion , 1 is language 2 = social
                    language_tones[lang_tone['tone_id']]+= lang_tone['score']
                #append the scores for the social tones
                for social_tone in response['document_tone']['tone_categories'][2]['tones']:
                    # 0 is emotion , 1 is language 2 = social
                    social_tones[social_tone['tone_id']]+= social_tone['score']
                sentences_chunk = "" # reset our sentence chunk

                number_of_chunks +=1

                if idx+1 == total_num_sentences : # finished
                    # compute the average
                    for emot_tone in response['document_tone']['tone_categories'][0]['tones']:
                        emotional_tones[emot_tone['tone_id']] /= number_of_chunks
                    for lang_tone in response['document_tone']['tone_categories'][1]['tones']:
                        language_tones[lang_tone['tone_id']] /= number_of_chunks
                    for social_tone in response['document_tone']['tone_categories'][2]['tones']:
                        social_tones[social_tone['tone_id']] /= number_of_chunks
                print(f"{idx+1} sentences analyzed")
            except Exception as e:
                print(e)
                sentences_chunk = "" # reset our sentence chunk to account for bad JSON

    return emotional_tones , language_tones , social_tones


def add_nlp_features(input_df,vc_type):
    """Feed in a dataframe that contains the company name, primary contact,
     and primary contacts twitter handle.
    Opens up the text file from that Twitter handle and runs it through
    the Watson APIs to get features on
    emotional, language, and social tones.
    VC type should be either 1 or 0

    Returns a the dataframe with these features appended."""
    total_rows = len(input_df)
    final_df = input_df.copy()

    language_tones_analytical = []
    language_tones_confident = []
    language_tones_tentative = []

    emotional_tones_anger = []
    emotional_tones_disgust = []
    emotional_tones_fear = []
    emotional_tones_joy = []
    emotional_tones_sadness = []

    social_tones_agreeableness_big5 = []
    social_tones_conscientiousness_big5 = []
    social_tones_emotional_range_big5 = []
    social_tones_extraversion_big5 = []
    social_tones_openness_big5 = []


    for idx,row in enumerate(input_df.iterrows()): # go through each founder
        handle = row[1]['Twitter_Username']
        print(handle)
        primary_contact = row[1]['Primary Contact']
        company = row[1]['Company Name']
        search = company +"-"+primary_contact+"-"+handle
        try: # see if we have tweets for this founder
            with open(f"../../data/raw/founders_tweets/vc_invest={vc_type}/{search}",'rb') as fp: # open up founder text
                text_data = pickle.load(fp)
            emotional, language, social = aggregated_tone_analyzer(text_data)
            emotional_tones_anger.append(emotional['anger'])
            emotional_tones_disgust.append(emotional['disgust'])
            emotional_tones_fear.append(emotional['fear'])
            emotional_tones_joy.append(emotional['joy'])
            emotional_tones_sadness.append(emotional['sadness'])

            language_tones_analytical .append(language["analytical"])
            language_tones_confident .append(language["confident"])
            language_tones_tentative.append(language["tentative"])

            social_tones_agreeableness_big5 .append(social["agreeableness_big5"])
            social_tones_conscientiousness_big5 .append(social["conscientiousness_big5"])
            social_tones_emotional_range_big5 .append(social["emotional_range_big5"])
            social_tones_extraversion_big5 .append(social["extraversion_big5"])
            social_tones_openness_big5.append(social["openness_big5"])

            if (idx % 10 == 0) & (idx >0):
                print(f"{idx/total_rows:.2%} finished")
        except Exception as e:# append NaN since we don't have text for this founder
            print(e)
            emotional_tones_anger.append("NaN")
            emotional_tones_disgust.append("NaN")
            emotional_tones_fear.append("NaN")
            emotional_tones_joy.append("NaN")
            emotional_tones_sadness.append("NaN")

            language_tones_analytical .append("NaN")
            language_tones_confident .append("NaN")
            language_tones_tentative.append("NaN")

            social_tones_agreeableness_big5 .append("NaN")
            social_tones_conscientiousness_big5 .append("NaN")
            social_tones_emotional_range_big5 .append("NaN")
            social_tones_extraversion_big5 .append("NaN")
            social_tones_openness_big5.append("NaN")


    # language Tones
    final_df['analytical'] = language_tones_analytical
    final_df['confident'] = language_tones_confident
    final_df['tentative'] = language_tones_tentative
    # emotional tones
    final_df['anger'] = emotional_tones_anger
    final_df['disgust'] = emotional_tones_disgust
    final_df['fear'] = emotional_tones_fear
    final_df['joy'] = emotional_tones_joy
    final_df['sadness'] = emotional_tones_sadness
    # Big 5 personality
    final_df['agreeableness_big5'] = social_tones_agreeableness_big5
    final_df['conscientiousness_big5'] = social_tones_conscientiousness_big5
    final_df['emotional_range_big5'] = social_tones_emotional_range_big5
    final_df['extraversion_big5'] = social_tones_extraversion_big5
    final_df['openness_big5'] =  social_tones_openness_big5

    # drop zeros

    final_df = final_df[final_df['analytical']!='NaN']
     #Convert columns to numeric type
    final_df = final_df.apply(pd.to_numeric, errors='ignore')

    return final_df


if __name__ == "__main__":
    # load in data
    #"../../data/processed/PitchBook_CA_VCInvest=1.csv"
    #../../data/processed/PitchBook_CA_VCInvest=0.csv
    pitchbook_df = pd.read_csv(sys.argv[1])
    final_df = add_nlp_features(pitchbook_df, vc_type=0) # add vc type here
    # save csv
    # "../../data/processed/PitchBook_CA_VCInvest=1_NLP-features.csv"
    #../../data/processed/PitchBook_CA_VCInvest=0_NLP-features.csv
    final_df.to_csv(sys.argv[2])
