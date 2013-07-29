#! /usr/bin/env python
# Daniel Alabi, Sweetie Song

import os
import datetime
import time
import pprint
import csv
import glob
import sys

from pymongo import MongoClient

try:
    # Get the environment variables for the port number and the directory for the flights csv file
    portNum = int(os.environ["PORT"])
    flightsDir = os.environ["FLIGHTS"]
except KeyError:
    print "Please set the environment variables $PORT and $FLIGHTS"
    sys.exit(1)

# Start a connection to the mongod instance
client = MongoClient("localhost", portNum)
coll = client["flying"]["flights"]


"""
takes in a line in the list and "prepares" a new doc to be inserted into the MongoDB
Collection
"""
def setAircraftAge(ls):
    nNumber = ls[0]
    age = int(ls[1])

    # store in flights collection in flying database
    coll.update( { "tailNum": nNumber }
                      , {"$set": {"age" : age}}
                      , multi= True)

"""
Add the Age of flights according to the MASTER directory
"""
def addAge():
    with open(flightsDir + "/matchedNNumbers.csv", "r") as f:
        # ifnore header
        f.readline()
        reader = csv.reader(f, delimiter=",")
        read = 0
        for line in reader:
            setAircraftAge(line)
            read += 1
            if read % 500 == 0:
                print read


if __name__ == "__main__":
    addAge();
    print "====FINISHED ADDING AGE===="

