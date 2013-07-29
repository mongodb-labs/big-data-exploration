PageRank-Flights
================

In this portion, we will show how to use MongoDB's MapReduce and Aggregation frameworks to compute PageRank on the flights dataset. Each node in PageRank will correspond to an airport. 

There is an in-depth [wiki-page](http://github.com/10gen-interns/big-data-exploration/wiki/Pagerank-on-Flights-Dataset) that details the steps we took to compute the PageRank of the commercial airports in the Flights dataset. The wiki page also describes what the various scripts in this repository do.

### Dependencies
* pymongo (`pip install pymongo` or `easy_install pymongo`) : >= 2.5
* python : >= 2.5, < 3.0
* MongoDB : >= 2.2

### Preformating

We assume that you've gone through the steps to import the Flights dataset into the _flights_ collection in the _flying_ database; if not, see `Basic-Flights/REAME.md`.

Currently each doc is for every flight, but our PageRank algorithm depends on an airport. The *transitionMatrix.py* script in src converts the flight documents into documents based on airports. It also formats the output as "value", which smoothes the transition to using the MongoDB MapReduce framework, since all outputs are labeled as "value". Lastly, the script also creates a transition matrix which tells the probability of going from that airport to any other airport (See [PageRank explanation](http://github.com/10gen-interns/big-data-exploration/wiki/PageRank-on-Flights-Dataset#making-a-graph-of-airports)). This script takes around 6 minutes. 

This creates a new collection titled *fpg_0*, which will be the input to the pagerank script. Even though there's over 6 million flights, there's only actually 319 airports that the flights use. This means that we're able to do a lot of the preformatting in memory in the python program.  

### Running the PageRank algorithm

In the src directory start a mongo shell instance connecting to port $PORT (`mongo --port <$PORT>`). Then in the shell, use the *flying* collection with `use flying`. Finally execute `load("iteration.js")`. This both loads and iterates the PageRank algorithm. There should be 19 iterations with the flight dataset we provide for you.

### Results

*airportsInfo.js* prints out each airport PageRank, state, code, and city information to be used in the airports.html page rendering the airport [PageRank map](http://s3.amazonaws.com/big-data-wiki/airports.html) in our results directory.
