PageRank-Flights
================

In this portion, we will show how to use Hadoop's MapReduce framework on another dataset, Twitter Memes. Each node in the graph will correspond to a web page on the internet.

There is an in-depth [wiki-page](http://github.com/10gen-interns/big-data-exploration/wiki/PageRank-on-Twitter-Memes-Dataset) that details the steps we took to import the data and compute the PageRank of the nodes in the Twitter Memes dataset. The wiki page also describes what the various scripts in this repository do.

### Dependencies
* pymongo (`pip install pymongo` or `easy_install pymongo`) : >= 2.5
* tldextract (`pip install tldextract`)
* MongoDB : >= 2.2
* Java : 1.6
* [mongo-hadoop](http://github.com/mongodb/mongo-hadoop)
* Amazon Web Service Account : Our examples utilize AWS, but you can possibly run it locally with minor changes instead
* [elastic-map-reduce client] (http://github.com/tc/elastic-mapreduce-ruby) 
* [s3cmd](http://s3tools.org/s3cmd)

### Setup
Export the variable `$BUCKET` to the amazon bucket location (ie, `export BUCKET=memes-bson`) where all your computations will take place. Build your mongo-hadoop and upload your build jars to an amazon bucket.

### Importing the Data into MongoDB

We have made all of our jar files necessary for hadoop and mongo-hadoop public in our memes-bson s3 bucket and they also exist in this repository. You can either copy these jars into your own bucket and make them public, or build your own jars, upload them and make them public. 

If you choose to use our files, run *src/update_s3.sh*. This script uploads all the necessary jar files and bootstrap scripts to s3 and makes them public.  

If you choose to utilize Amazon Web Service, you don't need to import the data locally, but this will show you how. Either way, remember the number of documents in your dataset, **TOTAL_DOCS**. 

======

You must have an instance of MongoDB running locally. Export the port number of the running mongod instace as `$PORT` (ie, `export PORT=27017`).

There are two ways to import the data.

------

#### MongoRestore with BSON
Download the bson file [memes.bson.zip](http://s3.amazonaws.com/big-data-wikie/memes.bson.zip) and unzip it.

Run `mongorestore --db twitter --collection memes --port $PORT <PATH TO>/memes.bson` to import the BSON file into the collection *memes* of the database *twitter*. 

#### Use our Input Scripts

Download any of the files from [Twitter Memes from Stanford Network Analysis Platform](http://memetracker.org/data/memetracker9.html) and unzip it. Export the variable `$MEMES` as the path to the directory where the txt files are stored.

Execute the `src/inputMongo.py` script. It should take around 2 hours. It will create a collection *memes* in the collection *twitter* with 15,312,736 documents. 

The only really important field is the `url` which becomes the `_id` value in our inputMongo.py script, so there's no need for further indices. 

### Prepping for PageRank

There's two different inputs to the PageRank algorithm:
* Any graph - Skip to Preformat
* A graph with no dead ends - to make this type of graph, following the instructions below.

To create a graph with no dead ends, all the dead ends must be erased, along with all the edges to those dead ends, and repeat until there are no dead ends.

The `src/RemoveDeadEnds` directory contains tools that do exactly that. Call `src/RemoveDeadEnds/removeAllDeadEnds.sh`. This might take over 5 hours and over 70 iterations. 

### Pre-Formating the Flights Dataset

All data must be converted from the format it's in, to a probability matrix with initial PageRank. You can either put the initial bson file or the output of the erase dead ends as input to this PreFormat MapReduce. Simply change the `mapred.input.dir=<S3 PATH TO YOUR INPUT>` on line 13 of `src/PreFormat/run_emr_job.sh` and change the `mapred.output.dir=<S3 PATH TO YOUR OUTPUT>` on line 19. If you used your own dataset, you must also set the number of `totalNodes` on line 21 to your own number. 

### Running the PageRank algorithm

In *src/PageRank/run_emr_job.sh*, specify your input and output directories on lines 12 and 13. Your input directory should be the output directory of your PreFormat. Your output directory **MUST end with a `/`**, this is necessary because during pagerank iteration, multiple job buckets will be created in this output directory. If you chose your own dataset, you must also set the number of `totalNodes` in `src/mapreduce/PageRankMapper.java`. The number cannot be passed in as a job config because the jobs are iteratively created in the main class.   

This iterative PageRank will continuously run the PageRank algorithm on the nodes until their values on average change less than 0.1%. This takes around 2.5 hours and 13 iterations with our dataset. You can view the progress in the stdout of the job on Elastic MapReduce. 

### Results

To analyze the results, you must download the PageRank outcome to your computer locally and upload them to MongoDB. 

In the MongoDB shell, you can load the `src/getMostPageRank.js` to print the top PageRank results and their corresponding quotes. 

**tldextract** is the module we use to correctly parse the url to obtain its domain and subtotal. This is used to see how much pagerank is gathered by each subdomain and domain. To create the subdomain and domain pagerank totals, export your `$PORT` number and `$COLLECTION` as the PageRank result collection name; the `src/domains.py` script will create the collections *subdomain* and *domain* in your *twitter* database. 

We've made two bubble diagrams based on the [bubble cloud d3 example](https://github.com/vlandham/bubble_cloud) with our dataset to show the top subdomains and domains.
