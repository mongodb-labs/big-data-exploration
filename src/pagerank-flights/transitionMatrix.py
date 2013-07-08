#! /usr/bin/python

import pymongo
import pprint

from pymongo import MongoClient

client = MongoClient("localhost", 27017)
coll = client["flying"]["flights"]
newcoll = client["flying"]["fpg_0"]

def convertToGraph():
    """
    convertToGraph ->
    converts the flights dataset to a graph.
    The flights dataset essentially contains only
    edges. So we had to create starting and end 
    nodes for each flight. The nodes correspond
    to the airports and the flights correspond to edges.
    The graph is stored in the "fpg_0" collection.
    """
    origstoouts = {}
    
    # get the total number of flights out of each airport to every other airport
    eachOutcome = coll.aggregate({"$group" : {"_id" : {"orig" : "$origAirportId", "dest" : "$destAirportId"}, "flew" : {"$sum" : 1}}})
    for r in eachOutcome["result"]:
        orig = r["_id"]["orig"]
        dest = r["_id"]["dest"]

        origstoouts.setdefault(orig, {"totalouts": 0, "toeach" : {}})
        origstoouts[orig]["toeach"].setdefault(dest, 0)
        origstoouts[orig]["toeach"][dest] += r["flew"]
        origstoouts[orig]["totalouts"] += r["flew"]

    # get the origAirportId
    origs = coll.distinct("origAirportId")
    allairports = frozenset(coll.distinct("destAirportId")) | frozenset(origs)

    # go through every airport
    # and set "totalouts" to 0 if totalouts doesn't exist
    
    # starting pagerank value for all airports
    st = 1/float(len(allairports))

    # for each distinct airport
    for airportId in allairports:
        doc = {"_id": str(airportId), "value" : {"pg" : st, "prs" : {}}}

	# need to get the number of total outgoing flights
        if airportId in origstoouts and "totalouts" in origstoouts[airportId]:
            totalCount = origstoouts[airportId]["totalouts"]
            for otherAirport, flew in origstoouts[airportId]["toeach"].items():
                doc["value"]["prs"][str(otherAirport)] = float(flew) / totalCount

            newcoll.insert(doc)

if __name__ == "__main__":
    # convert the Flights dataset to a graph for use in PageRank
    convertToGraph()    

