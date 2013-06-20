#! /usr/bin/env python
# Look at the cascades for arrDelay, particularly with aircrafts

import pymongo
import datetime
import pprint

from bson.code import Code
from pymongo import MongoClient

client = MongoClient("localhost", 27017)
collection = client["flying"]["flights"]

"""
For a given tailNum and scheduled arrival time for that flight. Find the
number of flights that was delayed at arrival time immediately following
"""
def findNumCascDelays(tailNum, crsArrTime):
    # find all the flights by that aircraft scheduled after the first
    # arrival time
    after = collection.find({"tailNum" : tailNum,
        "crsDepTime" : {"$gt" : crsArrTime}}).sort([("crsDepTime", 1)])

    if after.count() == 0:
        return 0
    else:
        num = 0
        # too many trips to use iter
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
    first = collection.find({"depDelay" : {"$lte":0}, "arrDelay" : {"$gt" : 0}})

    num = 0
    delays = 0
    for doc in first:
        delays += findNumCascDelays(doc["tailNum"], doc["crsArrTime"])
        num += 1
        if num % 10000 == 0:
            print doc["_id"]
    print delays
    print float(delays) / float(num)
