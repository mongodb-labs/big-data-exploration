#! /usr/bin/env python

"""
Script used to render the data that's used for the d3 display:
1. State flight traffic
 (a) How many flights leave each state?
 (b) How many flights arrive at each state?
2. Flight delays by state
 (a) Which states have on average the longest flight departure delays?
 (b) Which states have on average the longest flight arrival delays?
3. What time of week/year is best suited for travel?
   (has the least arrival delays)
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
    print "Please set the environment variable $PORT"
    sys.exit(1)

client = MongoClient("localhost", portNum)
flights = client["flying"]["flights"]


# Return the first n states with the most incoming/outgoing
# flights in the past year. This is different from getmostfrequentattr
# because the group by is by number of docs, not an attribute of the docs
def getStatesByFlights(aggregateId="origStateId", n=10):
    return flights.aggregate([
        {"$group" : {"_id" : "$"+aggregateId,
                     "numFlights" : {"$sum" : 1}}}
        , {"$sort" : {"numFlights" : -1}}
        , {"$limit" : n}
        ])

# Returns the first n items grouped by aggregateId
# and ordered in descending order of depDelay
def getmostfrequentattr(aggregateId="origStateId", aggregateValue="depDelay", agg="$avg", n=10):
    return flights.aggregate([
        {"$group" : {"_id": "$"+aggregateId,
                     "delay" : {agg : "$"+aggregateValue}}}
        , { "$sort" : {"delay" : -1} }
        , { "$limit" : n}
    ])

# Gets the hour of week with the most delays, this is special because of the compound index
def getCompoundId(aggregateValue="arrDelay", agg="$avg", n=10):
    return flights.aggregate([
        {"$group" : {"_id":
            {"w" : {"$dayOfWeek" : "$date"},
            "h" : {"$hour" : "$date"}},
            "delay" : {agg : "$"+aggregateValue}}}
        , {"$sort": {"delay" : -1}}
        , {"$limit" : n}
        ])

if __name__ == "__main__":

    print "===STATES WITH THE MOST FLIGHTS LEAVING================="
    for doc in getStatesByFlights()["result"]:
        pprint.pprint(doc)
    print "===STATES WITH THE MOST FLIGHTS ARRIVING================"
    for doc in getStatesByFlights("destStateId")["result"]:
        pprint.pprint(doc)

    print "===STATES WITH MOST DEPARTURE DELAYS===================="
    for doc in getmostfrequentattr()["result"]:
        pprint.pprint(doc)
    print "===STATES WITH MOST ARRIVAL DELAYS======================"
    for doc in getmostfrequentattr("destStateId", "arrDelay")["result"]:
        pprint.pprint(doc)

    print "===TIME OF WEEK WITH MOST ARRIVAL DELAYS================"
    for doc in getCompoundId()["result"]:
        pprint.pprint(doc)
    print "===DAY OF YEAR WITH MOST ARRIVAL DELAYS================="
    for doc in getmostfrequentattr("date", "arrDelay")["result"]:
        pprint.pprint(doc)
