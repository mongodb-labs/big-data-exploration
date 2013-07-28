Basic-Flights
=============

There is an in-depth [wiki page](http://github.com/10gen-interns/big-data-exploration/wiki/Basic-Analysis-on-Flights-Dataset) for actions and analysis of this dataset. Please refer to it for more details of the project, including the reasons behind the scripts and explanations of the sample analysis. 

### Dependencies
* pymongo (`pip install pymongo`)

### Resources

All the resources are zipped in the resources directory. 

Our sample dataset flights.zip is available in the resources directory. The dataset is 234MB zipped and it covers April 2012-March 2013, which is the most recent 12 months worth of data at the time of the project. You may also choose to download your own dataset from from the [Bureau of Transportation Statistics](http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236). However, it is imperative that your data include all the columns listed in [this
gist](http://gist.github.com/sweetieSong/60167000), because these fields are crucial to our analysis.

MatchedNNumbers.csv is a file of NNumbers matching the aircrafts that is in flights.zip. This is useful because it's significantly smaller than MASTER.txt.

MASTER.txt is the master registry of all aircrafts, it includes all details of every aircraft ever registered. 

### Importing the Data into MongoDB

Have an instance of MongoDB running locally. Export the port number of the running mongod instance as $PORT (ie, `export PORT=27017`). 
There's two methods of importing the data. 

======
#### MongoRestore with BSON 
Download the bson file [flights.bson.zip](http://s3.amazonaws.com/big-data-wiki/flights.bson.zip). 

Run `mongorestore --db flying --collection flights --port <$PORT> <PATH TO>/flights.bson` to import the BSON file into your MongoDB instance into the collection flights of the db flying. Then skip to to the aggregation topic. 

#### Use our input scripts

Download the file [flights.zip](http://s3.amazonaws.com/big-data-wiki/flights.zip) and unzip it. Export the variable $FLIGHTS as the path to the directory where all the csv from the flights.zip is stored. 

Execute the src/inputMongo.py script, it should take around 21 minutes to finish. It'll create a collection *flights* in the db *flying* with 6,155,748 documents.

It also creates the most relevant indices that will be used for this dataset. 

##### Adding Age of aircraft
If you ran our input script, you'll need to create an age column not included in the FAA's data. We'll also so calculations with the of the aircraft, which isn't included in the original csv's. There's two options for this:
1. Download [MASTER.txt.zip](http://s3.amazonaws.com/big-data-wiki/MASTER.txt.zip) and put it into the same directory as the csv files from flights. Unzip the MASTER.txt.zip and run createNNumberColl.py. This is particularly useful if you downloaded your own dataset as MASTER.txt is the master registry of all aircrafts. However, this takes upward of 8 hours.  
2. **The preferred method** Download [matchedNNumbers.csv](http://s3.amazonaws.com/big-data-wiki/matchedNNumbers.csv) and put it into the directory with the csv files. Run the matchedNNumbers.py. We've already matched the NNumbers in the MASTER.txt and created a matchedNNumbers.csv in resources solely for our flights.zip dataset. This adds the age to the existing entries in the *flights* collection. However, this still around 2 hours.  

### Aggregation and results 

The *aggregation_examples.py* script runs many of the most basic flight compilation flights. It displays the essentials of the aggregation framework by interchanging the group variables and the measurements. 

*cascadingDelays.py* is another python script that finds the number of delays caused by each late arrival of the aircraft. It might take a while to run (upwards of 2 hours as well), but on average, there's only 1 late flight per flight that leaves on time and arrives late.

The results directory contains much of the graphic rendering of the aggregation results from *datad3.py* (datad3 has a limit of outputting 10 results per call so that the results doesn't overwhelm you), all of these utilize [d3](http://d3js.org). However, most of these contain some modification and scaling before they can be used directed in the javascript. The four major graphs are:
* departandarrivedelays.html - a map based on [Choropleth](http://bl.ocks.org/4060606) displaying the state arrival and departure delays
* flightsinout.html - a map showing the number of flights coming in and out of a state
* weeklyDelays.png - modified after the [Day/Hour Heatmap](http://bl.ocks.org/tjdecke/5558084) showing the arrival delay average according to the hour of departure.
* yearlyDelays.png - a variation of the [Calendar View](http://blo.ocks.org/mbostock/4063318) to display the daily arrival delay over the course of the year in the data set. 

