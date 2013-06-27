#! /usr/bin/env python

import pymongo
import pprint

from pymongo import MongoClient

client = MongoClient("localhost", 27017)
coll = client["twitter"]["memes2"]
newcoll = client["twitter"]["memes3"]
mpcoll = client["twitter"]["mpg_0"]

# pages collection
n = 8
pcoll = client["twitter"]["char"+str(n)]

def eraseDeadEnds():
    """
    Deletes links which are dead ends
    """
    c = 0
    changed = False
    bulkData = []

    for doc in coll.find(timeout=False):
        for link in doc["value"]["links"]:
            nfound = pcoll.find({"_id" : link[0:n]
                                 , "urls" : link}).count()

            if nfound == 0:
                doc["value"]["links"].remove(link)
                changed = True

        if len(doc["value"]["links"]) == 0:
        	pcoll.update({"_id" : doc["_id"][0:n]}, {"$pull" : {"urls" : doc["_id"]}})
        else:
			bulkData.append(doc)

        c += 1
        if c % 1000 == 0:
            if len(bulkData) > 0:
                newcoll.insert(bulkData)
            bulkData = []
            print "Searched through " + str(c) + " docs so far."

    return changed

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
        			"pg" : initialPG,
        			"ls" : doc["value"]["links"],
         			"ptr" : 1.0 /len(doc["value"]["links"])
        			}
        		}
        bulkData.append(pgDoc)
        c += 1

        if c % 1000 == 0:
            mpcoll.insert(bulkData)
            bulkData = []
            print "Inserted " + str(c) + " documents so far"


if __name__ == "__main__":
	changed = True
	while (changed):
		changed = eraseDeadEnds()

	createTM()
