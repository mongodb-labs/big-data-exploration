#! /usr/bin/python

"""
pigpagerank.py -->
Embedded pig script to 
compute the pagerank of the graph stored in params['docs_in'].
Outputs the result to params['docs_out']. 

Modified from: http://techblug.wordpress.com/2011/07/29/pagerank-implementation-in-pig/
"""


from org.apache.pig.scripting import *

P = Pig.compile("""
REGISTER /Users/danielalabi/pig_libraries/mongo-2.10.1.jar
REGISTER /Users/danielalabi/pig_libraries/mongo-hadoop-core-1.1.0-SNAPSHOT.jar
REGISTER /Users/danielalabi/pig_libraries/mongo-hadoop-pig-1.1.0-SNAPSHOT.jar

previous_pagerank = LOAD '$docs_in'
USING com.mongodb.hadoop.pig.MongoLoader('url:chararray, pagerank:float, links:{t: (link:chararray)}');

outbound_pagerank = FOREACH previous_pagerank
GENERATE pagerank / COUNT(links) AS pagerank, FLATTEN(links) AS to_url;

new_pagerank = FOREACH (COGROUP outbound_pagerank BY to_url, previous_pagerank
BY url INNER)
GENERATE
group AS url,
(1-$d) + $d * SUM(outbound_pagerank.pagerank) AS pagerank,
FLATTEN (previous_pagerank.links) AS links,
FLATTEN (previous_pagerank.pagerank) as previous_pagerank;

pagerank_diff = FOREACH new_pagerank GENERATE ABS(previous_pagerank - pagerank);

max_diff = FOREACH (GROUP pagerank_diff ALL) GENERATE MAX (pagerank_diff);

STORE new_pagerank
INTO '$docs_out'
USING com.mongodb.hadoop.pig.MongoStorage();

-- write into a file
STORE max_diff
INTO '$max_diff';

""");

params = {'d': '0.85', 'docs_in' : 'mongodb://localhost/test.pagerank_data_sample2'}

# max iterations is 100
for i in range(100):
    out = 'mongodb://localhost/test.pagerank_data_' + str(i+1)
    params['max_diff'] = "diff_store_" + str(i+1)
    params['docs_out'] = out
    stats = P.bind(params).runSingle()
    if not stats.isSuccessful():
        raise 'failed'
    
    max_diff_value = float(str(stats.result('max_diff').iterator().next().get(0)))
    print "max_diff_value = " + str(max_diff_value)
    if max_diff_value < 0.01:
        print "done at iteration " + str(i)
        break
    
    params['docs_in'] = out
