PageRank-Flights
================

In this portion, we will show how to use MongoDB's MapReduce and Aggregation frameworks to compute PageRank on the flights dataset. Each node in PageRank will correspond to an airport. 

There is an in-depth [wiki-page](http://github.com/10gen-interns/big-data-exploration/wiki/Pagerank-on-Flights-Dataset) that details the steps we took to compute the PageRank of the commercial airports in the Flights dataset. The wiki page also describes what the various scripts in this repository do.

------

## Dependencies
* MongoDB : >= 2.2
* python : >= 2.5, < 3.0
* pymongo (`pip install pymongo` or `easy_install pymongo`) : >= 2.5

------

## Pre-Formating the Flights Dataset

We assume that you've gone through the steps to import the Flights dataset into the _flights_ collection in the _flying_ database; if not, see `Basic-Flights/REAME.md`.

At this point, every document in the _flights_ collection corresponds to a single flight. But our goal is to compute the PageRank of commercial airports in the flights network. In order to do so, we need a new collection where every document corresponds to exactly one commercial airport in the U.S. The `src/preformat.py` converts the flight documents into documents based on airports. It also formats the output as `value`, which smoothes the transition to using the MongoDB MapReduce
framework, since all outputs from MongoDB MapReduce are embedded in a `value` field. Lastly, the script also creates a transition matrix (not literally, but in form of MongoDB documents) which tells the probability of going from one airport to any other airport (See [PageRank explanation](http://github.com/10gen-interns/big-data-exploration/wiki/PageRank-on-Flights-Dataset#making-a-graph-of-airports)). This script takes around 6 minutes to run. 

The resulting collection *fpg_0* will be the input to the pagerank script. Even though there's over 6 million flights, there's only actually 319 airports that the flights use. This means that we were able to do a lot of the preformatting in memory in the python program.  

------

## Running the PageRank algorithm

In the `src` directory, start a mongo shell instance connecting to port `$PORT` (`mongo --port <$PORT>`). Then in the shell, use the *flying* collection with `use flying`. Finally execute `load("iteration.js")`. This executes the PageRank algorithm on the flights network stored in *fpg_0*. The PageRank for all commercial airports should converge after 19 iterations (assuming you're using the initial flight dataset we provided).

------

## Results

`airportsInfo.js` prints out each airport PageRank, state, code, and city information to be used in the d3 airports.html page rendering the airport [PageRank map](http://s3.amazonaws.com/big-data-wiki/airports.html) in our results directory.
