#! /usr/bin/env python

import pymongo
import pprint

from pymongo import MongoClient

client = MongoClient("localhost", 27017)
coll = client["twitter"]["memes70"]
mpcoll = client["twitter"]["mpg_0"]

def createTM():
    """
    Inserts the documents in the "pagerank" format
    """
    initialPG = 1.0 / coll.count()
    c = 0
    bulkData = []

    for doc in coll.find(timeout=False):
    	pgDoc = {"_id" : doc["_id"]
                    , "value" : {
                        "pg" : initialPG
                        , "ls" : doc["value"]["links"]
                        , "ptr" : 1.0 /len(doc["value"]["links"])
            		}
                }
        bulkData.append(pgDoc)
        c += 1

        if c % 1000 == 0:
            mpcoll.insert(bulkData)
            bulkData = []
            print "Inserted " + str(c) + " documents so far"

    if len(bulkData) > 0:
        mpcoll.insert(bulkData)


if __name__ == "__main__":
    createTM()
