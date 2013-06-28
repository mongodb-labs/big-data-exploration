#! /usr/bin/env python

import pymongo
import pprint

from pymongo import MongoClient

client = MongoClient("localhost", 27017)

def eraseDeadEnds(numRound):
    """
    Deletes links which are dead ends
    """
    coll = client["twitter"]["memes"+numRound]
    roundPlusOne = numRound + 1
    newcoll = client["twitter"]["memes"+roundPlusOne]

    c = 0
    changed = False
    bulkData = []

    for doc in coll.find(timeout=False):
        for link in doc["value"]["links"]:
            nfound = coll.find({"_id" : link}).count()

            if nfound == 0:
                doc["value"]["links"].remove(link)
                changed = True

        if len(doc["value"]["links"]) >= 0:
			bulkData.append(doc)

        c += 1
        if c % 1000 == 0:
            if len(bulkData) > 0:
                newcoll.insert(bulkData)
            bulkData = []
            print "Searched through " + str(c) + " docs so far."

    return changed


if __name__ == "__main__":
	changed = True
    numRound = 3
	while (changed):
		changed = eraseDeadEnds(numRound)
        numRound += 1
