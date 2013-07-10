#! /usr/bin/env python

import pymongo
import pprint

from pymongo import MongoClient

client = MongoClient("localhost", 27017)
coll = client["twitter"]["memes"]
newcoll = client["twitter"]["mpg_0"]

"""
Deletes links which are dead ends
"""
def eraseDeadEnds():
    for doc in coll.find():
        doc["idLinks"] = []

        for link in doc["links"]:

            # If this link is dead end
            result = coll.find({"url" : link})
            if result.count() > 0:
                doc["idLinks"].append(result[0]["_id"])

        coll.save(doc)

# Create the transition matrix
def createTM():
    initialPG = 1.0 / coll.count()

    for doc in coll.find():
        pgDoc = {"_id" : doc["_id"]
                , "value" : {
                    "pg" : initialPG,
                    "idl" : doc["idLinks"],
                    "ptr" : 1.0 /len(doc["idLinks"])
                    }
                }
        newcoll.save(pgDoc)


if __name__ == "__main__":
    eraseDeadEnds()
    createTM()
