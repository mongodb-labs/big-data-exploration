#! /usr/bin/env python

import pymongo
import pprint

from pymongo import MongoClient

client = MongoClient("localhost", 27017)

def eraseDeadEnds(numRound):
    """
    Deletes links which are dead ends
    """
    coll = client["twitter"]["memes"+str(numRound)]
    roundPlusOne = numRound + 1
    newcoll = client["twitter"]["memes"+str(roundPlusOne)]

    c = 0
    changed = False
    bulkData = []

    for doc in coll.find(timeout=False):
        foundLinks = []
        for link in doc["value"]["links"]:
            nfound = coll.find({"_id" : link}).count()

            if nfound != 0:
                foundLinks.append(link)
            else:
                changed = True

        if len(foundLinks) > 0:
            doc["value"]["links"] = foundLinks
            bulkData.append(doc)

        c += 1
        if c % 1000 == 0:
            if len(bulkData) > 0:
                newcoll.insert(bulkData)
            bulkData = []
            print "Searched through " + str(c) + " docs so far."

    if len(bulkData) > 0:
        newcoll.insert(bulkData)

    return changed


if __name__ == "__main__":
    numRound = 69
    changed = True
    while (changed):
        changed = eraseDeadEnds(numRound)
        numRound += 1
