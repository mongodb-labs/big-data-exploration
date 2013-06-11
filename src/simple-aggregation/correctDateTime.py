#! /usr/bin/python

from pymongo import MongoClient
import datetime

# crs times are not included because there is no more than 12 hour difference
# so the crsDep and crsArr times were correct from the input script
correspondingTimeFields = {"depTime" : "wheelsOff"
        , "wheelsOff": "wheelsOn"}


if __name__ == "__main__":
    client = MongoClient("localhost", 27017)
    coll = client["flying"]["flights"]
    totalChanged = 0

    # correct the dates for every doc in the collection, since we can't tell
    # which ones are wrong
    for doc in coll.find():

        changed = False
        # Check that these fields are included in the doc (they should be)
        if "crsDepTime" in doc and "depDelay" in doc:
            flewDate = doc["crsDepTime"] + datetime.timedelta(minutes=doc["depDelay"])
            # if the scheduled and actual times are on different days
            if flewDate.day != doc["crsDepTime"].day:
                changed = True
                doc["depTime"] = flewDate
        if "arrDelay" in doc and "crsArrTime" in doc:
            arrDate = doc["crsArrTime"] + datetime.timedelta(minutes=doc["arrDelay"])
            if arrDate.day != doc["crsArrTime"].day:
                changed = True
                doc["arrTime"] = arrDate

        # depTime corrects wheelsOff before wheelsOff corrects wheelsOn
        for before in sorted(correspondingTimeFields):
            after = correspondingTimeFields[before]
            if before in doc and after in doc and doc[before] > doc[after]:
                changed = True
                doc[after] = doc[after] + datetime.timedelta(days=1)

        if changed:
            totalChanged += 1
            coll.update({"_id" : doc["_id"]}, doc)

    print totalChanged
