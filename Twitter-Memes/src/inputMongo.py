#! /usr/bin/env python

import os
import datetime
import time
import pprint
import glob
import sys

from pymongo import MongoClient

portNum = 27017
memesDir = ""
# Get the environ variables for the port num and directory for the quotes txt file(s)
try:
    portNum = int(os.environ["PORT"])
    memesDir = os.environ["MEMES"]
except KeyError:
    print "Please set the environment variables $PORT and $MEMES."
    sys.exit(1)

client = MongoClient("localhost", portNum)
coll = client["twitter"]["memes"]


def parseFile(f):
    """
    For a given file f, iterate through the file to insert twitter docs
    The format of each doc will be:
    "_id" : ObjectId default
    "url" : http://blogs/..." the URL/P of the document
    "time" : ISODate("2012-09-09 23-45-23") the timestamp of the document
    "quotes" : ["quote1", "quote2"] the quotes array associated
    "links" : ["http://globs...", "http://cnn.com/sss"] links array to other docs
    """
    doc = {"quotes" : [], "links" : []}
    bulkDocs = []

    recCount = 0
    for line in f:
        lineSplit = line.strip().split(None, 1)

        # Skip invalid lines
        if len(lineSplit) < 2: continue

        firstLetter = lineSplit[0]

        # the first letter denotes the type of line
        if firstLetter == "P":
            # When there's a previous doc and this line starts a new doc, insert the old one
            if "url" in doc:
                bulkDocs.append(doc)

            doc = {"url" : lineSpit[1], "quotes" : [], "links" : []}

            if len(bulkDocs) % 10000 == 0:
                recCount += 10000
                coll.insert(bulkDocs)
                bulkDocs = []
                print "Have seen and inserted " + str(recCount) + " records"

        elif firstLetter == "T":
            # Get the time of the post
            pointInTime = time.strptime(lineSplit[1].rstrip(), "%Y-%m-%d %H:%M:%S")
            doc["time"] = datetime.datetime(*pointInTime[:6])
        elif firstLetter == "Q":
            doc["quotes"].append(lineSplit[1])
        elif firstLetter == "L":
            doc["links"].append(lineSplit[1])
        else:
            print "ERROR:" + line

    # insert the last doc
    coll.insert(bulkDocs)


# This is useful if the mongod instance disconnects and needs to
# continue inserting after a certain known point
# skip the first n nodes in the file f
# <->
# skip the first n "P"'s
def skipsomenodes(f, n):
    c = 0
    for line in f:
        if c == (n-1):
            break
        c += 1

        lineSplit = line.strip().split(None, 1)
        if len(lineSplit) < 2: continue

        if lineSplit[0] == "P": c += 1
    print "skipped "+str(c)+" documents"
    return f


if __name__ == "__main__":
    # Iterate through the files in the Flights directory

    for fname in glob.glob(memesDir + "/quotes_*.txt"):
        with open(fname, "r") as f:
            # might need skipsomenodes(f, n) if resuming from previous point
            parseFile(f)
            print "====== FINISHED IMPORTING TWITTER MEMES ======"
