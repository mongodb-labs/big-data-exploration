-- pig script to calculate current pagerank of graph
-- in one iteration
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
