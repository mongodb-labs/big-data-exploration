Basic-Flights
=============

There is an in-depth [wiki page](http://github.com/10gen-interns/big-data-exploration/wiki/Basic-Analysis-on-Flights-Dataset) that details the steps we took to 
analyze the Flights dataset and what the various scripts in this repository do.

## Dependencies

* MongoDB : >= 2.2
* python :  >= 2.5, < 3.0
* pymongo (`pip install pymongo` or `easy_install pymongo`) : >= 2.5

------


## Importing the Data into MongoDB

You must have an instance of MongoDB running locally. Export the port number of the running mongod instance as `$PORT` (ie, `export PORT=27017`). 
There are two ways to import the data.


### 1. MongoRestore with BSON 
Download the bson file [flights_bson.zip](http://s3.amazonaws.com/big-data-wiki/flights_bson.zip). 

Run `mongorestore --db flying --collection flights --port $PORT <PATH TO>/flights_original.bson` to import the BSON file into the collection *flights* of the database *flying*. 

### 2. Use our Input Scripts

If you'd like to use our input scripts instead, download the original csv files first. Either download the dataset of flights in the last year that we used at [flights.zip](http://s3.amazonaws.com/big-data-wiki/flights.zip) and unzip it, or download your own dataset from the [FAA](http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236) with all the fields according to [this gist](https://gist.github.com/sweetieSong/6016700).

Export the variable `$FLIGHTS` as the path to the directory where all the csv from the flights.zip is stored (ie, `export FLIGHT=/User/Downloads/flight`). 

Execute the `src/inputMongo.py` script, it should take around 21 minutes to finish. It'll create a collection *flights* in the db *flying* with 6,155,748 documents. The script also creates the most relevant indices that will be used for this dataset. 

##### Adding Age of aircraft
If you ran our input script, you'll probably want to create an _age_ field not included in the FAA's data. We'll also do some calculations with the age of aircrafts, which isn't included in the original csv's. There's two options for this:

1. Download [MASTER.txt.zip](http://s3.amazonaws.com/big-data-wiki/MASTER.txt.zip) and put it into the same directory as the csv files from flights. Unzip the MASTER.txt.zip and run `src/masterNNumbers.py`. This is particularly useful if you downloaded your own dataset since MASTER.txt is the master registry of all aircrafts. However, this takes more than 8 hours to finish.

2. **The preferred method**: Download [matchedNNumbers.csv](http://s3.amazonaws.com/big-data-wiki/matchedNNumbers.csv) and put it into the directory with the csv files. Run the `src/matchedNNumbers.py`. We've already matched the NNumbers in the MASTER.txt and created a matchedNNumbers.csv solely for our flights.zip dataset. This adds the _age_ to the existing entries in the *flights* collection. However, this also takes around 2 hours.  

It is not completely necessary to create an `age` field, but it is used in `src/aggregation_examples.py` as mentioned below and will be helpful to your analysis of the data. 


------

## Aggregations

The `src/aggregation_examples.py` script contains a basic aggregation query. With different parameters for the group variables and measurements, the aggregation query returns various results demonstrating the powerful effects of simple queries in the MongoDB aggregation framework. 

`src/cascadingDelays.py` is another python script that finds the number of delays caused by each late arrival of the aircraft. It might take a while to run (upwards of 2 hours as well), but on average, there's only **0.189** consecutive delays per flight. See [wiki](https://github.com/10gen-interns/big-data-exploration/wiki/Basic-Analysis-on-Flights-Dataset#cascading-delays) for more details on this.

------

## Results

The `results` directory contains much of the graphic rendering of the aggregation results from `src/datad3.py` (datad3 has a limit of outputting 10 results per call so that the results doesn't overwhelm you). All of the graphics utilize [d3](http://d3js.org). However, most of these contain some modification and scaling before they can be used directly in the javascript. The four major graphs are:

* `departandarrivedelays.html` : a map based on [Choropleth](http://bl.ocks.org/4060606) displaying the state arrival and departure delays.
* `flightsinout.html` : a map showing the number of flights coming in and out of a state.
* `weeklyDelays.png` : modified after the [Day/Hour Heatmap](http://bl.ocks.org/tjdecke/5558084) showing the arrival delay average according to the hour of departure.
* `yearlyDelays.png` : a variation of the [Calendar View](http://blo.ocks.org/mbostock/4063318) to display the daily arrival delay over the course of the year in the data set. 
