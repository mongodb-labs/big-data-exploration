#! /usr/bin/env python

import os
import tldextract
import sys

from pymongo import MongoClient

portNum = 27017
collName = ""
try:
    portNum = int(os.environ["PORT"])
    collName = os.environ["COLLECTION"]
except KeyError:
    print "Please set the environment variables $PORT an $COLLECTION"
    sys.exit(1)

client = MongoClient("localhost", portNum)
coll = client["twitter"][collName]


"""
For a dictionary of id to pg value, insert it as a doc into MongoDB
with given name of collection
"""
def insertDicIntoMongo(d, name):
    newcoll = client["twitter"][name]
    bulkData = []
    for key,value in d.iteritems():
        bulkData.append({"_id" : key, "pg" : value})
        if len(bulkData) % 5000 == 0:
            newcoll.insert(bulkData)
            bulkData = []
    newcoll.insert(bulkData)
    print "===== FINISHED", name, "INSERTIONS======"


"""
For each document in the PageRank results
add its pagerank to both its subdomain and domain
"""
def sumPageRank():
    subDomains = {}
    domains = {}

    past = 0
    # Get the subdomain and domain and add its pagerank to each
    for doc in coll.find(timeout=False):
        pg = doc["pg"]
        ext = tldextract.extract(doc["_id"])

        sub = '.'.join(ext[:2])
        d = ext.domain

        subDomains[sub] = subDomains.setdefault(sub, 0.0) + pg
        domains[d] = domains.setdefault(d, 0.0) + pg

        past += 1
        if past % 100000 == 0:
            print past, "document pg have been added"

    print "======FINISHED PAGERANK ADDITIONS======"

    insertDicIntoMongo(subDomains, "subDomainsPG")
    insertDicIntoMongo(domains, "domainsPG")


if __name__ == "__main__":
    sumPageRank()
