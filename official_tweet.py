import pandas as pd
import tweepy
import random
import socket
import json
import time


def scrape(window_id):
    words = ['bts', 'ipl', 'johnnydepp', 'elonmusk', 'covid']
    final_res = []
    for word in words:
        tweets = tweepy.Cursor(api.search_tweets,
                               q=word,
                               tweet_mode='extended').items(75)
        list_tweets = [tweet for tweet in tweets]
        for tweet in list_tweets:
            username = tweet.user.screen_name
            hashtags = tweet.entities['hashtags']
            hashtag = ''
            if len(hashtags) > 0:
                hashtag = hashtags[0]['text']
                if hashtag.lower() != word.lower():
                    continue
            else:
                continue
            send_details = [username, window_id, hashtag]
            print(send_details)
            final_res.append(send_details)
    return final_res


if __name__ == '__main__':

    consumer_key = 'g8qhoRayrGJoKGrisuN6xNCHL'

    consumer_secret = 'LWsqp9jBPy8rBc20WShYeLCAsz21DKIDb29tx7MTEN8MkeCsDu'

    access_key = '1106363006-jsnIwQYqjwaaoCitn2ziK2Uv1ZRVGTQXngD6KCd'

    access_secret = '7SXlXBPHzf9tqXh5wXoYhNR0XfHWvY4k4oHjK0WpZOrl2'
    # access_key = '1289176512273367045-cKg9rIBMBY7p24OnovDOsLHVLKq5qH'
    # access_secret = 'TnLQJRtpxsl1DOAXsgDZOVEEWKuYM1rbxVUSrCBWii1tO'
    # consumer_key = 'kfav8r32TkZ5jp5ZlEiAzkuJn'
    # consumer_secret = 'JitxgZb35p7rA5kE2jM7w8BspR2PIVOKRwiPiaLlNAqzQDGOVj'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 6100))
    s.listen(1)
    print(f"Waiting for connection on port {6100}...")
    connection, address = s.accept()
    print(f"Connected to {address}")
    print('socket is ready')
    print("Received request from: " + str(address))
    # Enter Hashtag and initial date
    for i in range(1, 7):
        data1 = scrape(i)
        # number of tweets you want to extract in one run
        # print('Scraping has completed!')
        # data1 = [['SnoSnoflingan', 'Sweden, Finland, UK and somewhere in between.', 'COVID19'], ['redboybroken', '', 'COVID19'], ['Idl3', "Esperto di nulla, ma curioso di tutto.\n👁\n🌍🌏🌎\nI'm not anonymous, my name is available to the public, but heavily redacted.\n\nhttps://t.co/eaQszqvVYU", 'COVID19'],
        # ['santu183sg', '', 'IPL']]
        total_lines = len(data1)
        send_batch = (json.dumps(data1) + '\n').encode()
        try:
            connection.send(send_batch)  # send the payload to Spark
        except BrokenPipeError:
            print(
                "Either batch size is too big for the dataset or the connection was closed")
        except Exception as error_message:
            print(f"Exception thrown but was handled: {error_message}")
    time.sleep(5)
