#! /usr/bin/env python

"""
Script used to answer some or all of the following questions:
1. What kinds of airplanes have the most delays? Consider arrival delay
    and departure delay different.
 (a) How old are these airplanes? mean, mode?
 (b) Where do they leave from mostly?
 (c) What airline companies do they belong to?
2. What kinds of airports have the most delays?
 (a) What's the airplane load (no. of airplanes that leave these airports)?
"""


import datetime, time
import pprint
import os
import sys

from pymongo import MongoClient

portNum = 27017
try:
    portNum = int(os.environ["PORT"])
except KeyError:
    print "Please set the environment variables $PORT"
    sys.exit(1)

client = MongoClient("localhost", portNum)
flights = client["flying"]["flights"]

"""
mostdelayedflights ->
returns the most delayed flights
"""
def mostdelayed(n, withages=False):
    # finds the most delayed flights (during either departure or arrival or both)
    # sorts first by arrDelay since departure delays are the most common
    q = {"age" : {"$exists" : True}} if withages else {}
    return flights.find(q,
                        {"_id":1
                         ,"arrDelay":1
                         , "depDelay":1
                         , "carrier":1
                         , "origCity":1
                         , "destCity":1
                         , "age": 1}).sort([("arrDelay", -1)]).sort([("depDelay", -1)]).limit(n)

# Returns the first n items grouped by attr
# and ordered in descending order of sortBy
def getmostfrequentattr(attr="origAirport", sortBy="depDelay", agg="$avg", n=10):
    return flights.aggregate([
        {"$group" : {"_id": "$"+attr,
                     "delay" : {agg : "$"+sortBy}}}
        , { "$sort" : {"delay" : -1} }
        , { "$limit" : n}
    ])

if __name__ == "__main__":
    print "======MOST DELAYED FLIGHTS===="
    d = mostdelayed(5)
    for doc in d:
        pprint.pprint(doc)
    d = mostdelayed(5, True)
    print "======MOST DELAYED FLIGHTS WITH AGE===="
    for doc in d:
        pprint.pprint(doc)
    print "===AIRPORTS WITH MOST DEPARTURE DELAYS===================="
    for doc in getmostfrequentattr()["result"]:
        pprint.pprint(doc)
    print "===AIRPORTS WITH MOST ARRIVAL DELAYS================="
    for doc in getmostfrequentattr("destAirport", "arrDelay")["result"]:
        pprint.pprint(doc)
    print "===CARRIERS WITH MOST DEPARTURE DELAYS===================="
    for doc in getmostfrequentattr("carrier", "depDelay")["result"]:
        pprint.pprint(doc)
    print "===CARRIERS WITH MOST ARRIVAL DELAYS===================="
    for doc in getmostfrequentattr("carrier", "arrDelay")["result"]:
        pprint.pprint(doc)
