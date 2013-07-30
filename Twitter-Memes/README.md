PageRank-Flights
================

In this portion, we will show how to use Hadoop's MapReduce another dataset, Twitter Memes. Each node in PageRank will correspond to a document. 

There is an in-depth [wiki-page](http://github.com/10gen-interns/big-data-exploration/wiki/PageRank-on-Twitter-Memes-Dataset) that details the steps we took to import the data and compute the PageRank of the Twitter Memes dataset. The wiki page also describes what the various scripts in this repository do.

### Dependencies
* pymongo (`pip install pymongo` or `easy_install pymongo`) : >= 2.5
* MongoDB : >= 2.2
* Java : 1.6
* Amazon Web Service Account : Our examples utilize AWS, but you can possibly run it locally instead with minor changes.
* [elastic-map-reduce client] (http://github.com/tc/elastic-mapreduce-ruby) 
* [s3cmd](http://s3tools.org/s3cmd)

### Importing the Data into MongoDB

You must have an instance of MongoDB running locally. Export the port number of the running mongod instace as `$PORT` (ie, `exporrt PORT=27017`).

There are two ways to import the data.
======

#### MongoRestore with BSON
Download the bson file [memes.bson.zip](http://s3.amazonaws.com/big-data-wikie/memes.bson.zip) and unzip it.

Run `mongorestore --db twitter --collection memes --port $PORT <PATH TO>/memes.bson` to import the BSON file into the collection *memes* of the database *twitter*. 

#### Use our Input Scripts

Download any of the files from [Twitter Memes from Stanford Network Analysis Platform](http://memetracker.org/data/memetracker9.html) and unzip it. Export the variable `$MEMES` as the path to the directory where the txt files are stored.

Execute the `src/inputMongo.py` script, it should take around 2 hours. It will create a collection *memes* in the collection *twitter* with 15,312,736 documents. 

The only really important field is the `url` which becomes the `_id` in out inputMongo.py script, so there's no need for further indices. 

### Prepping for PageRank

There's two different inputs to the PageRank algorithm:
* A graph with no dead ends - to make this type of graph, following the instructions below.
* Any graph - Skip to Preformat

To create a graph with no dead ends, all the dead ends must be erased, along with all the edges to those dead ends, and repeat until there are no dead ends.

The src/RemoveDeadEnds does exactly that. ~~~TODO DANIEL WRITE HERE ~~~ 

We have made all of our jar files necessary for hadoop and mongo-hadoop public in out memes-bson s3 bucket. You can either leave the references in place, or download them and copy them to your own bucket.

### Pre-Formating the Flights Dataset

We assume that you've gone through the steps to import the Flights dataset into the _flights_ collection in the _flying_ database; if not, see `Basic-Flights/REAME.md`.

At this point, every document in the _flights_ collection corresponds to a single flight. But our goal is to compute the PageRank of commercial airports in the flights network. In order to do so, we need a new collection where every document corresponds to exactly one commercial airport in the U.S. The *transitionMatrix.py* script in `src/` converts the flight documents into documents based on airports. It also formats the output as "value", which smoothes the transition to using the MongoDB MapReduce framework, since all outputs from MongoDB MapReduce are embedded in a "value" field. Lastly, the script also creates a transition matrix (not literally, but in form of MongoDB documents) which tells the probability of going from one airport to any other airport (See [PageRank explanation](http://github.com/10gen-interns/big-data-exploration/wiki/PageRank-on-Flights-Dataset#making-a-graph-of-airports)). This script takes around 6 minutes to run. 

The resulting collection *fpg_0* will be the input to the pagerank script. Even though there's over 6 million flights, there's only actually 319 airports that the flights use. This means that we were able to do a lot of the preformatting in memory in the python program.  

### Running the PageRank algorithm

In the `src` directory, start a mongo shell instance connecting to port `$PORT` (`mongo --port <$PORT>`). Then in the shell, use the *flying* collection with `use flying`. Finally execute `load("iteration.js")`. This executes the PageRank algorithm on the flights network stored in *fpg_0*. The PageRank for all commercial airports should converge after 19 iterations (assuming you're using the initial flight dataset we provided).

### Results

*airportsInfo.js* prints out each airport PageRank, state, code, and city information to be used in the airports.html page rendering the airport [PageRank map](http://s3.amazonaws.com/big-data-wiki/airports.html) in our results directory.
