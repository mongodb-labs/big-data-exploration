#! /usr/bin/python
# Daniel Alabi, Sweetie Song

import os
import datetime
import time
import pprint
import csv
import glob

from pymongo import MongoClient

client = MongoClient("localhost", 27017)
coll = client["flying"]["flights"]

# ALL fields
fields = ["year","quarter","month","dayOfMonth","dayOfWeek"
, "date","carrierId","airlineId","carrier","tailNum"
, "flightNum","origAirportId","origCityId","origAirport"
, "origCity","origStateId","origState","origWAC","destAirportId"
, "destCityId","destAirport","destCity","destStateId","destState"
,"destWAC","crsDepTime","depTime","depDelay","taxiOut","wheelsOff"
,"wheelsOn","taxiIn","crsArrTime","arrTime","arrDelay","cancelled"
,"cancelCode","diverted","crsElapsedTime","elapsedTime"
,"airTime","numFlights","distance", "carrierDelay",
"weatherDelay","nasDelay","securityDelay","lateAircraftDelay"
,"numDivAirportLandings","divReachedDest","divElapsedTime"
,"divArrDelay","divDistance"]

# integer fields
integerFields = ["year","quarter","month","dayOfMonth"
,"dayOfWeek","airlineId","flightNum","origAirportId"
, "origCityId", "origWAC","destAirportId","destCityId","destWAC"
,"depDelay" ,"taxiOut","taxiIn","arrDelay","crsElapsedTime"
,"elapsedTime" ,"airTime","numFlights","distance"
,"carrierDelay","weatherDelay","nasDelay","securityDelay"
,"lateAircraftDelay" ,"numDivAirportLandings"
,"divElapsedTime","divArrDelay","divDistance"]

# boolean fields
booleanFields = ["diverted", "divReachedDest"]
# time fields
timeFields = ["crsDepTime", "depTime", "wheelsOff"
, "wheelsOn", "crsArrTime", "arrTime"]
correspondingTimeFields = {"crsDepTime": "crsArrTime"
                           , "depTime" : "arrTime"
                           , "wheelsOff": "wheelsOn"}

"""
lsToDoc -
takes in a ls and 'prepares' a new doc to be inserted
into a MongoDB collection
"""
def lsToDoc(ls):
    doc = dict(zip(fields, ls))

    # first parse the date
    if "date" not in doc:
        return {}
    date = time.strptime(doc["date"], "%Y-%m-%d")
    doc["date"] = datetime.datetime(date.tm_year, date.tm_mon, date.tm_mday)
    date = doc["date"]

    # delete "cancelled"; don't need it for now or ever
    del doc["cancelled"]

    for attr, value in doc.items():
        # delete any empty columns
        if not isinstance(value, datetime.datetime) and len(value) == 0:
            del doc[attr]

        elif attr in integerFields:
            doc[attr] = int(float(value))
        elif attr in booleanFields:
            doc[attr] = bool(value)
        elif attr in timeFields:
            hour, mins = int(value[:len(value)-2]), int(value[len(value)-2:])
            if hour == 24:
                hour = 0
                date = date + datetime.timedelta(days=1)
            doc[attr] = datetime.datetime(date.year, date.month, date.day, \
                                          hour, mins)

        elif attr == "cancelCode":
            # cancellation codes: A, B, C, D. Set as binary
            # mapping { A:1, B:2, C:3, D:4 }
            doc[attr] = ord(value)-64

    # handle arrive time
    for attr, value in correspondingTimeFields.items():
        if attr in doc and value in doc and doc[attr] > doc[value]:
            doc[value] = doc[value] + datetime.timedelta(days=1)

    return doc


if __name__ == "__main__":
    # Iterate through the files in the Flights directory
    month = ""
    year = ""
    for fname in glob.glob("/shared/Flights/*" + month + year + ".csv"):    
        with open(fname, "r") as f:
            f.readline()
            reader = csv.reader(f, delimiter=",", quotechar="\"")
            # skip the first line in the file
            for line in reader:
                doc = lsToDoc(line)
                if len(doc) > 0:
                    coll.insert(doc)

