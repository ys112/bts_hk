# -*- coding: utf-8 -*-
"""
Created on Fri Jun  8 18:27:54 2018

@author: ys112
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Jun  1 23:38:06 2018

@author: ys112
"""
from tweepy.auth import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
from dateutil import parser
from http.client import IncompleteRead
import time
from datetime import datetime
import sys
import pyodbc

cons_key = 'DF0CnjnudahLS2g5z0iq9Fa2y'
cons_secret = 'lRAqc7HuSV1AVas4TyrQdf4o8zV08cFjcpCtRY0FXNeqrympMs'
auth = OAuthHandler(cons_key, cons_secret)
auth.set_access_token('995906967410393089-DBuV6coojcJULWJKu0B6PE1nXKzyYqg', 'xiJIsu4JibawLAa2HxrFiX6IHzQVrPgUPpRklee65patv')

 
def store_data(tweets):
    server = 'tcp:yys2017.database.windows.net,1433' 
    database = 'test' 
    username = 'ys112@yys2017' 
    password = 'P@ssw0rd' 
    db = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    #db=mysql.connect(host="localhost", user="root", passwd="12345", db="twitter", charset="utf8mb4")
    cursor = db.cursor()
    insert_query = "INSERT INTO bts_1 (id, text, userid, username, userlang, userlocation,createddate) VALUES (?,?,?,?,?,?,?)"
    for t in tweets:
        cursor.execute(insert_query,t)
    db.commit()
    cursor.close()
    db.close()
    print("stored")
    return

class listener(StreamListener):
    tweets = []
    error_count = 0
    
    def on_status(self, status):
        if(not status.retweeted and 'RT @' not in status.text):
            tweet_id = status.id
            try:
                text = status.extended_tweet["full_text"]
            except AttributeError:
                text = status.text
            userid = status.user.id
            userscreen = status.user.screen_name
            userlang = status.user.lang
            userlocation = status.user.location
            createddate = parser.parse(str(status.created_at))
            listener.tweets.append((tweet_id, text, userid, userscreen, userlang, userlocation, createddate)) #add to list of tuple
            if(len(listener.tweets)== 500):
                print("500 tweets")
            if(len(listener.tweets) == 1000):  #if 1000 tweets , store it
                print("1000 tweets storing")
                print("stored time: " + str(datetime.now()))
                listener.error_count = 0
                store_data(listener.tweets)
                listener.tweets.clear()
        
    def on_error(self,status):
        listener.error_count +=1
        if(listener.error_count == 5):
          sys.exit(0)
        print(status)
        print("on_error time: " + str(datetime.now()))
        time.sleep(15)
        return False

def start():
    error_count = 0
    while True:
        try:
            twitterStream= Stream(auth, listener())
            twitterStream.filter(track=['#bts','#BTS', '#방탄소년단','방탄소년단'])
                                
        except KeyboardInterrupt:
            # allow exiting with ctrl-c
            twitterStream.disconnect()
            print(" time: " + str(datetime.now()))
            break
        
        except Exception as e:
            error_count = error_count + 1
            
            print()
            print(e)
            print("Error time: " + str(datetime.now()))
            twitterStream.disconnect()
            if(error_count == 5):
                sys.exit(0)
            time.sleep(10)
            continue

start()
