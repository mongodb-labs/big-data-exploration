#! /usr/bin/env python
# Daniel Alabi, Sweetie Song

import os
import datetime
import time
import pprint
import csv
import glob

from pymongo import MongoClient

# Get the environment variables for the port number and the directory for the flights csv file
portNum = int(os.environ['PORT'])
flightsDir = os.environ["FLIGHTS"]

# Start a connection to the mongod instance
client = MongoClient("localhost", portNum)
coll = client["flying"]["flights"]


"""
takes in a line in the list and 'prepares' a new doc to be inserted into the MongoDB
Collection
"""
def setAircraftAge(ls, mongoCollClient):
    # fields to include
    # note: "n-number" is assumed to start with an N
    fields = ["nNumber", "yearMfr"]
    fieldcols = [0, 4]

    # Only get the fields that we want
    newls = [ls[i].strip() for i in fieldcols]
    doc = dict(zip(fields, newls))

    # yearMfr should be stored as an integer
    if len(doc["yearMfr"]) > 0:
        doc["yearMfr"] = int(doc["yearMfr"])
        # calculate age of flights
        age = 2013 - doc["yearMfr"]
        # store in flights collection in flying database
        mongoCollClient.update( { "tailNum": "N"+doc["nNumber"] }
                          , {"$set": {"age" : age}}
                          , multi= True)

"""
Add the Age of flights according to the MASTER directory
"""
def addAge(flightsDir, mongoCollClient):
    with open(flightDir + "/MASTER.txt", "r") as f:
        # ifnore header
        f.readline()
        reader = csv.reader(f, delimiter=",")
        read = 0
        for line in reader:
            setAircraftAge(line, mongoCollClient)
            read += 1
            if read % 1000 == 0:
                print read


if __name__ == "__main__":
    addAge(flightsDir, coll);
    print "====FINISHED ADDING AGE===="
