#! /usr/bin/env python
# Look at the cascades for arrDelay, particularly with aircrafts

import pymongo
import datetime
import pprint
import os
import sys

from bson.code import Code
from pymongo import MongoClient

try:
    portNum = int(os.environ["PORT"])
except KeyError:
    print "Please set the environment variable $PORT"
    sys.exit(1)

client = MongoClient("localhost", 27017)
collection = client["flying"]["flights"]

"""
It's easier to think about finding cascading delays recursively
than iteratively. But for efficiency purposes, we decided to
implement the function below (findNumCascDelays) iteratively.

First, we obtain the set S
where S = { flights that left early but arrived late }
then for every x in S, we obtain
the number of cascading delays that x caused
(on the same aircraft).
"""


def findNumCascDelays(tailNum, crsArrTime):
    """
    tailNum -> tail number of aircraft delayed
    crsArrTime -> scheduled arrival time for delayed aircraft
    """
    # find all the flights by that aircraft scheduled after the first
    # arrival time
    after = collection.find({"tailNum" : tailNum,
        "crsDepTime" : {"$gt" : crsArrTime}}).sort([("crsDepTime", 1)])

    if after.count() == 0:
        return 0
    else:
        num = 0

        for trip in after:
            if ("depDelay" in trip and "arrDelay" in trip \
                    and "lateAircraftDelay" in trip ):
                if (trip["depDelay"] > 0 and trip["arrDelay"] > 0 \
                    and trip["lateAircraftDelay"] > 0):
                        num += 1
                        continue
            break
    return num

if __name__ == "__main__":
    # find all the flights that leave on time but arrive late, thereby causing
    # cascading delays
    first = collection.find({"depDelay" : {"$lte":0}, "arrDelay" : {"$gt" : 0}}, timeout=False)

    num = 0
    delays = 0
    for doc in first:
        delays += findNumCascDelays(doc["tailNum"], doc["crsArrTime"])
        num += 1
        if num % 10000 == 0:
            print num, "late arrival flights have caused", delays, "delays"
    print "On average, per late flight causes", float(delays) / float(num), "cascading delays"
