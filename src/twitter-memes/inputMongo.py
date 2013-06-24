#! /usr/bin/env python
# Daniel & Sweet

import os
import datetime
import time
import pprint
import glob

from pymongo import MongoClient

client = MongoClient("localhost", 27017)
coll = client["twitter"]["memes"]


"""
For a given file f, iterate through the file to insert twitter docs
"""
def parseFile(f):

	""" The format of each doc will be:
	"_id" : ObjectId default
	"url" : http://blogs/..." the URL/P of the document
	"time" : ISODate("2012-09-09 23-45-23") the timestamp of the document
	"quotes" : ["quote1", "quote2"] the quotes array associated
	"links" : ["http://globs...", "http://cnn.com/sss"] links array to other docs
	"""
	doc = {"quotes" : [], "links" : []}

	recCount = 0
	for line in f:

		lineSplit = line.strip().split(None, 1)
                if len(lineSplit) < 2:
                        continue
		firstLetter = lineSplit[0]

		# the first letter denotes the type of line
		if firstLetter == "P":
			# When there's a previous doc and this line starts a new doc, insert the old one
			if "url" in doc:
				coll.insert(doc)
				doc = {"quotes" : [], "links" : []}

			doc["url"] = lineSplit[1]

			recCount += 1
			if recCount % 100000 == 0:
				print "Have seen " + recCount + " records"

		elif firstLetter == "T":
			pointInTime = time.strptime(lineSplit[1].rstrip(), "%Y-%m-%d %H:%M:%S")
			doc["time"] = datetime.datetime(*pointInTime[:6])
		elif firstLetter == "Q":
			doc["quotes"].append(lineSplit[1])
		elif firstLetter == "L":
			doc["links"].append(lineSplit[1])
		else:
			print "ERROR:" + line

	# insert the last doc
	coll.insert(doc)


if __name__ == "__main__":
    # Iterate through the files in the Flights directory
    month = ""
    year = ""

    for fname in glob.glob("/Users/danielalabi/Memes/quotes_*" + year + "-" + month + "*.txt"):
        print fname
        with open(fname, "r") as f:
        	parseFile(f)
