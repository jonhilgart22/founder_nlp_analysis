#! usr/bin python
# coding: utf-8
import pandas as pd
import requests
import tweepy
import yaml
import os
import json
import numpy as np
__author__ = "Jonathan HIlgart"

# Read PitchBook data in
# Start with companies that HAVE received VC funding.
# These are the 1 companies (VC investment =1)


def read_in_data():
    """Read in PitchBook data for companies that HAVE received VC funding.
    ALso, merge these dataframes together and return the merged df."""
    print("Reading in data")

    vc_general_info_df = pd.read_excel(
        "../../data/raw/CA_VC_PitchBook/Company_General_Information.xlsx",header=6)
    # interesting columns= Company ID (primary key), Description, Company Name,
    # HQ Post Code, Primary Industry Code, Primary Contact, Year Founded


    vc_last_financing_df = pd.read_excel(
        "../../data/raw/CA_VC_PitchBook/Last_Financing_Details.xlsx",header=6)
    # interesting columns = Company ID ( primary key), Company Name,
    # Growth Rate, Size Multiple, last financing date,
    # last financing Size, Last financing valuation, Last Financing Deal Type 2

    # Note : Only want series A or later, filter OUT the seed rounds

    vc_company_financials_df = pd.read_excel(
        "../../data/raw/CA_VC_PitchBook/Public_Company_Financials.xlsx",header=6)
    # Interesting columns are NOTHING

    vc_social_web_df =  pd.read_excel(
        "../../data/raw/CA_VC_PitchBook/Social_and_Web_Presence.xlsx",header=6)
    # interesting columns = company id (primary key), company name, growth rate,
    #  size multiple, majestic referring domains
    # facebook likes, Tiwtter followers, Employees, Total raised


    vc_general_info_colDrop_df = vc_general_info_df[["Company ID",
        "Description", "Company Name", "HQ Post Code", "Primary Industry Code",
        "Primary Contact", "Year Founded", "Active Investors","HQ Location"]]


    vc_last_financing_colDrop_df =vc_last_financing_df[["Company ID",
                    "Growth Rate", "Size Multiple",
    "Last Financing Date","Last Financing Size","Last Financing Valuation",
                    "Last Financing Deal Type 2 "]]


    vc_social_web_colDrop_df  =vc_social_web_df [["Company ID",  "Growth Rate",
                        "Size Multiple", "Majestic Referring Domains",
        "Facebook Likes", "Twitter Followers", "Employees", "Total Raised"]]


    final_vc_df = vc_general_info_colDrop_df.merge(vc_last_financing_colDrop_df,
                on='Company ID').merge(vc_social_web_colDrop_df, on='Company ID')
    final_vc_df .drop([
                  'Growth Rate_y','Size Multiple_y'],axis=1,inplace=True)

    final_vc_df.rename(columns={'Growth Rate_x':'Growth Rate',"Size Multiple_x":'Size Multiple',
                               "Company Name_x":"Company Name"},inplace=True) # rename cols
    return final_vc_df


# # Drop companies missing the zip code, year founded, and primary contact
# - Can't impute this
# - Last Financing Valuation: Drop this ( too many nulls)

def drop_cols(final_vc_df):
    """Drop columns that are not necessary for the analysis"""
    print("Dropping irrelevant columns")

    final_vc_financeTypeFilter_df = final_vc_df.loc[(
        final_vc_df['Last Financing Deal Type 2 ']!='Seed') &
                    (final_vc_df['Last Financing Deal Type 2 ']!='Angel'),: ]
    final_vc_dropFinanceZipYear_df = final_vc_financeTypeFilter_df.loc[
        (final_vc_financeTypeFilter_df['HQ Post Code'].isnull()==False) &
        (final_vc_financeTypeFilter_df['Year Founded'].isnull()==False) &
        (final_vc_financeTypeFilter_df['Primary Contact'].isnull()==False),: ]
    # first drop last financing valuation (not enough data)
    final_vc_dropFinanceZipYear_df.drop(['Last Financing Valuation'], axis=1,
                                        inplace=True)


    # Add VCinvest = 1
    ones = np.array([1 for _ in range(len(final_vc_dropFinanceZipYear_df ))]).reshape(-1,1)
    final_vc_dropFinanceZipYear_df.loc[:,'VC_invested'] = ones
    return final_vc_dropFinanceZipYear_df


# Impute missing values
#  Growth Rate : impute median
#  Size Multiple: imput median
#  Last Financing Date : dont't really need this ( only need for angel companies)
#  Last Financing Size : impute median
#  Majestic Referring Domains: Impute Median
#  Facebook Like: Impute median
#  Twitter followers: impute emdian
#  Employees: impute median
#  Total raised: impute median

def impute_median_values(final_vc_dropFinanceZipYear_df):
    """For the input df, input the median values for the missing data"""
    print("Imputing missing values")
    median_values={}
    for row in final_vc_dropFinanceZipYear_df.describe(): # get median values
        if row =='Last Financing Valuation': # don't have enough data for this
            pass
        else:
            median_values[row]=final_vc_dropFinanceZipYear_df.describe()[row]["50%"]
    imputed_final_df = final_vc_dropFinanceZipYear_df.copy()
    for key in median_values: # update the nan values with the median
        updated_col = final_vc_dropFinanceZipYear_df.loc[:,key].copy()
        updated_col = updated_col.fillna(median_values[key])
        imputed_final_df.loc[:,key] = updated_col
    return imputed_final_df

def username_search(name, company, state):
    """Run a search on twitter for the given name. Returns the
    first username (should be the most relevant).
    Looks to match a state location with the state locatio nof the company

    First try searching for the person's name + company.
    If that does not work, try just searching for the
    person's name"""
    state = state.lower()

    credentials = yaml.load(open(os.path.expanduser('~/.ssh/api_credentials.yml')))
    auth = tweepy.OAuthHandler(credentials['twitter']['consumer_key'], credentials['twitter']['consumer_secret'],)
    auth.set_access_token(credentials['twitter']['token'], credentials['twitter']['token_secret'])
    api = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    tweets = api.search_users(q=str(name)+" "+str(company))


    try: # search the name and the company
        tweets[0].screen_name
        for result in tweets:
            if state in result.location.lower().split(" "):
                return result.screen_name

    except Exception as e: # try just the name
        try:

            tweets = api.search_users(q=name)
            for result in tweets:
                if state in result.location.lower():
                    return result.screen_name


        except Exception as e:
            print(e)
            return "NaN"



# ### Add the Twitter username to the pandas df for the VC dataframe

def get_twitter_usernames(imputed_final_df):
    """Return the Twitter username from the """
    print("Searching for Twitter usernames")
    twitter_usernames_vc_df = []
    for idx,row in enumerate(imputed_final_df.iterrows()):
        location = row[1]['HQ Location'].split(",")[1].strip(" ")
        company = row[1]['Company Name']
        founder = row[1]['Primary Contact']

        if idx%100 ==0:
            print(f"Finished {idx}")
    twitter_usernames_vc_df.append(username_search(founder,company, location ))

    imputed_final_df['Twitter_Username'] = twitter_usernames_vc_df
    # Drop rows where we couldn't find the Twitter username
    final_vc_df = ifinal_vc_df = imputed_final_df[
        (imputed_final_df.Twitter_Username!='NaN') | (imputed_final_df.Twitter_Username!='None') ]
    return final_vc_df



if __name__ =="__main__":
    initial_df = read_in_data()
    dropped_df = drop_cols(initial_df)
    imputed_df = impute_median_values(dropped_df)
    add_username_df = get_twitter_usernames(imputed_df)


    add_username_df.to_csv("../../data/processed/PitchBook_CA_VCInvest=1.csv")
