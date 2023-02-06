import tweepy
import pandas as pd


def run_tweet_elt():
    api_key = "JCVd2Wlzasle7bd9g2JglC2f6"
    api_secret_key = "UcYzoo4j0ilD1djBi2GUgltguCV2UAnGKBn1GB1vu3wAtrVR8l"
    access_token = "899042534370009088-lkicMY0gPfvXlimgAmrtmCwcSiHf3Pb"
    access_token_secret = "GEmidnfieL7yG9TIGloxQOxuArVHXBePkvahAlKmDAYPj"

    # authorize the API Key
    authentication = tweepy.OAuthHandler(api_key, api_secret_key)
    # authorization to user's access token and access token secret
    authentication.set_access_token(access_token, access_token_secret)
    api = tweepy.API(authentication)

    search_query = "sncf"
    no_of_tweets = 1000

    try:
        # The number of tweets we want to retrieve from the search
        tweets = api.search_tweets(q=search_query, count=no_of_tweets)

        # Pulling Some attributes from the tweet
        attributes_container = [[tweet.user.name, tweet.created_at, tweet.favorite_count, tweet.source, tweet.text] for
                                tweet in tweets]

        # Creation of column list to rename the columns in the dataframe
        columns = ["User", "Date Created", "Number of Likes", "Source of Tweet", "Tweet"]

        # Creation of Dataframe
        tweets_df = pd.DataFrame(attributes_container, columns=columns)
        tweets_df.to_csv("tweet_sncf.csv")
    except BaseException as e:
        print('Status Failed On,', str(e))
