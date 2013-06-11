#! /usr/bin/python

import datetime, time
import csv

from pymongo import MongoClient

client = MongoClient("localhost", 27017)
coll = client["flying"]["nnumbers"]

# fields to include
# note: "n-number" is assumed to start with an N
fields = ["nNumber", "serialNumber", "name", "yearMfr", "cancelDate"]
fieldcols = [0, 1, 4, 11, 17]

"""
lsToDoc - 
takes in a list and 'prepares' a new doc to be inserted into the MongoDB
Collection
"""
def lsToDoc(ls):
    newls = [ls[i].strip() for i in fieldcols]
    doc = dict(zip(fields, newls))

    # yearMfr should be stored as an integer
    if len(doc["yearMfr"]) > 0:
        doc["yearMfr"] = int(doc["yearMfr"])
    else:
        del doc["yearMfr"]

    # cancelDate should be parsed as yearmonthday (no delimiter)
    if len(doc["cancelDate"]) > 0:
        t = time.strptime(doc["cancelDate"], "%Y%m%d")
        doc["cancelDate"] = datetime.datetime(t.tm_year, t.tm_mon, t.tm_mday)
    else:
        del doc["cancelDate"]

    return doc


if __name__ == "__main__":
    with open("/shared/Flights/DEREG.txt", "r") as f:
        # ignore header
        f.readline()
        reader = csv.reader(f, delimiter=",")
        for line in reader:
            doc = lsToDoc(line)
            if len(doc) > 0:
                coll.insert(doc)
        
