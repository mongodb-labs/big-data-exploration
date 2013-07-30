#! /usr/bin/env python

import os
import datetime
import time
import pprint
import csv
import glob
import sys

from pymongo import MongoClient

portNum = 27017
flightsDir = ""
# Get the environment variables for the port number and the directory for the flights csv file
try:
    portNum = int(os.environ["PORT"])
    flightDir = os.environ["FLIGHTS"]
except KeyError:
    print "Please set the environment variables $PORT and $FLIGHTS."
    sys.exit(1)

# Start a connection to the mongod instance
client = MongoClient("localhost", portNum)
coll = client["flying"]["flights"]


# ALL fields
fields = ["year", "quarter", "month", "dayOfMonth", "dayOfWeek"
, "date", "carrierId", "airlineId", "carrier", "tailNum"
, "flightNum", "origAirportId", "origCityId", "origAirport"
, "origCity", "origStateId", "origState", "origWAC", "destAirportId"
, "destCityId", "destAirport", "destCity", "destStateId", "destState"
, "destWAC", "crsDepTime", "depTime", "depDelay", "taxiOut", "wheelsOff"
, "wheelsOn", "taxiIn", "crsArrTime", "arrTime", "arrDelay","cancelled"
, "cancelCode", "diverted", "crsElapsedTime", "elapsedTime"
, "airTime", "numFlights", "distance", "carrierDelay"
, "weatherDelay", "nasDelay", "securityDelay", "lateAircraftDelay"
, "numDivAirportLandings", "divReachedDest", "divElapsedTime"
, "divArrDelay", "divDistance"]

# integer fields
integerFields = ["year", "quarter", "month", "dayOfMonth"
, "dayOfWeek", "airlineId", "flightNum", "origAirportId"
, "origCityId", "origWAC", "destAirportId", "destCityId", "destWAC"
, "depDelay", "taxiOut", "taxiIn","arrDelay", "crsElapsedTime"
, "elapsedTime", "airTime", "numFlights", "distance"
, "carrierDelay", "weatherDelay", "nasDelay", "securityDelay"
, "lateAircraftDelay", "numDivAirportLandings"
, "divElapsedTime", "divArrDelay", "divDistance"]

# boolean fields
booleanFields = ["diverted", "divReachedDest"]
# time fields
timeFields = ["crsDepTime", "depTime", "wheelsOff"
, "wheelsOn", "crsArrTime", "arrTime"]
correspondingTimeFields = {"crsDepTime": "crsArrTime"
                           , "depTime" : "arrTime"
                           , "wheelsOff": "wheelsOn"}

# fields that should have indices on them to speed up computations
indexFields = ["depDelay", "arrDelay", "tailNum", "origStateId"
        , "depStateId"]


"""
Takes in a doc, and corrects all the dates that are initially incorrect.
This includes arriving the next day and or delays causing next day flights
"""
def correctDays(doc):

    # Some flights actually leave and or arrive the day or days after
    # their schedule.
    if "crsDepTime" in doc and "depDelay" in doc:
        flewDate = doc["crsDepTime"] + datetime.timedelta(minutes=doc["depDelay"])
        # if the scheduled and actual times are on different days
        if flewDate.day != doc["crsDepTime"].day:
            doc["depTime"] = flewDate
    if "crsArrTime" in doc and "arrDelay" in doc:
        arrDate = doc["crsArrTime"] + datetime.timedelta(minutes=doc["arrDelay"])
        if arrDate.day != doc["crsArrTime"].day:
            doc["arrTime"] = arrDate

    # If the arrival time is before the departure time, this means that
    # the plane actually arrived the next day, need to change manually
    for attr, value in correspondingTimeFields.items():
        if attr in doc and value in doc and doc[attr] > doc[value]:
            doc[value] = doc[value] + datetime.timedelta(days=1)

    return doc


"""
lsToDoc -
takes in a line from the csv and "prepares" a new doc to be inserted
into a MongoDB collection
"""
def lsToDoc(ls):
    doc = dict(zip(fields, ls))

    # first parse the date, docs missing the date is useless
    if "date" not in doc:
        return {}
    date = time.strptime(doc["date"], "%Y-%m-%d")
    doc["date"] = datetime.datetime(date.tm_year, date.tm_mon, date.tm_mday)
    date = doc["date"]

    # delete "cancelled", it becomes 0 in cancelCode
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
            # All time fields aside from date of flight comes in the form
            # hrmn where hour is in 1 to 24 and min is 00 to 59
            hour, mins = int(value[:len(value)-2]), int(value[len(value)-2:])
            if hour == 24:
                hour = 0
                # this is the next day
                date = date + datetime.timedelta(days=1)
            doc[attr] = datetime.datetime(date.year, date.month, date.day, \
                                          hour, mins)

        elif attr == "cancelCode":
            # cancellation codes: A, B, C, D. Set as binary
            # mapping { A:1, B:2, C:3, D:4 }
            doc[attr] = ord(value)-64

    doc = correctDays(doc)

    return doc


"""
Given the port number, flights directory, and mongo collection client
import the data in the files into mongoDB using bulk insertion
"""
def importFiles():
    # Iterate through the files in the Flights directory
    for fname in glob.glob(flightsDir + "/*.csv"):
        print "Importing file ", fname
        with open(fname, "r") as f:

            # skip the first line in the file, it's a column header line
            f.readline()

            # bulkData is used for bulk insertion to speed up the insertion time
            bulkData = []
            reader = csv.reader(f, delimiter=",", quotechar="\"")
            numDone = 0
            for line in reader:

                # Turn a line into a MongoDB document
                doc = lsToDoc(line)
                # Discard lines that aren't useful if they don't have date
                if len(doc) > 0:
                    bulkData.append(doc)

                    # Insert the data once the list reaches 1,000
                    if len(bulkData) % 1000 == 0:
                        numDone += 1000
                        coll.insert(bulkData)
                        if numDone % 100000 == 0:
                            print str(numDone) + " documents have been inserted so far."
                        bulkData = []

            # Add the straggler data
            coll.insert(bulkData)


def createIndices():
    for index in indexFields:
        print "Creating index for ", index
        coll.create_index(index)


if __name__ == "__main__":
    importFiles()
    print "====FINISHED IMPORTING===="

    createIndices()
    print "====FINISHED CREATING INDICES==== "
